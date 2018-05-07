defmodule OnsagerCore.Ring do
  @moduledoc """
  Manages a node's local view of partition ownership. State is encapsulated
  in the CHState struct (Consistent Hashing State struct) and exchanged between nodes
  via a gossip protocol.
  """

  defmodule CHState do
    defstruct [
      :nodename,
      :vclock,
      :chring,
      :meta,
      :clustername,
      :next,
      :members,
      :claimant,
      :seen,
      :rvsn
    ]

    @type t :: %CHState{
            nodename: term,
            vclock: VC.vclock(),
            # change this after implementing chring
            chring: OnsagerCore.CHash.chash(),
            meta: OnsagerCore.Ring.MetaEntry.t(),
            clustername: {term, term},
            next: [{integer, term, term, [module], :awaiting | :complete}],
            members: [{node, {OnsagerCore.Ring.member_status(), VC.vclock(), [{atom, term}]}}],
            claimant: term,
            seen: [{term, VC.vclock()}],
            rvsn: VC.vclock()
          }
  end

  defmodule MetaEntry do
    defstruct [:value, :lastmod]

    @type t :: %MetaEntry{value: term, lastmod: non_neg_integer}
  end

  alias OnsagerCore.VectorClock, as: VC
  alias OnsagerCore.CHash, as: CH
  alias OnsagerCore.{Gossip}

  @type member_status :: :joining | :valid | :invalid | :leaving | :exiting | :down
  @opaque onsager_core_ring :: CHState.t()
  @type chstate :: onsager_core_ring
  @type pending_change ::
          {node, node, :awaiting | :complete} | {:undefined, :undefined, :undefined}
  @type resize_transfer :: {{integer, term}, :ordsets.ordset(node), :awaiting | :complete}
  @type ring_size :: non_neg_integer
  @type partition_id :: non_neg_integer

  def set_tainted(ring) do
    update_meta(:onsager_core_ring_tainted, true, ring)
  end

  def check_tainted(ring = %CHState{}, msg) do
    exit_env = Application.get_env(:onsager_core, :exit_when_tainted, false)

    case {get_meta(:onsager_core_ring_tainted, ring), exit_env} do
      {{:ok, true}, true} ->
        OnsagerCore.stop(msg)
        :ok

      {{:ok, true}, false} ->
        # TODO log
        :ok

      _ ->
        :ok
    end
  end

  @spec nearly_equal(chstate, chstate) :: boolean
  def nearly_equal(ring_a, ring_b) do
    test_vc = VC.descends(ring_b.vclock, ring_a.vclock)
    ring_a2 = %{ring_a | vclock: :undefined, meta: :undefined}
    ring_b2 = %{ring_b | vclock: :undefined, meta: :undefined}
    test_ring = Map.equal?(ring_a2, ring_b2)
    test_vc && test_ring
  end

  @spec is_primary(chstate, {CH.index_as_int(), node}) :: boolean
  def is_primary(ring, idx_node) do
    owners = all_owners(ring)
    Enum.member?(owners, idx_node)
  end

  @spec chash(chstate) :: CH.chash()
  def chash(%CHState{chring: chash}) do
    chash
  end

  def set_chash(state = %CHState{}, chash) do
    %{state | chring: chash}
  end

  @doc """
  Produces a list of all nodes that are members of the cluster
  """
  @spec all_members(chstate) :: [term]
  def all_members(%CHState{members: members}) do
    get_members(members)
  end

  def members(%CHState{members: members}, types) do
    get_members(members, types)
  end

  @doc """
  List of all active, as in not marked down, cluster members
  """
  def active_members(%CHState{members: members}) do
    get_members(members, [:joining, :valid, :leaving, :exiting])
  end

  @doc """
  List of members guaranteed safe for requests
  """
  def ready_members(%CHState{members: members}) do
    get_members(members, [:valid, :leaving])
  end

  @doc """
  Provide ownership information in the form of {index, node} pairs
  """
  @spec all_owners(chstate) :: [{integer, term}]
  def all_owners(state) do
    CH.nodes(state.chring)
  end

  def all_preflists(state, n) do
    keys = for {i, _owner} <- all_owners(state), do: <<i + 1::160>>
    for key <- keys, do: Enum.slice(preflist(key, state), 0, n)
  end

  @doc """
  Given two rings, return the list of owners with differing ownership
  """
  @spec diff_nodes(chstate, chstate) :: [node]
  def diff_nodes(state_1, state_2) do
    all_owners_0 = Enum.zip(all_owners(state_1), all_owners(state_2))
    all_diff = for {{i, n1}, {i, n2}} <- all_owners_0, n1 !== n2, do: [n1, n2]
    :lists.usort(List.flatten(all_diff))
  end

  def equal_rings(
        state_a = %CHState{chring: ring_a, meta: meta_a},
        state_b = %CHState{chring: ring_b, meta: meta_b}
      ) do
    md_a = Enum.sort(Map.to_list(meta_a))
    md_b = Enum.sort(Map.to_list(meta_b))

    case md_a === md_b do
      false -> false
      true -> ring_a === ring_b
    end
  end

  @doc """
  Used only when this node is creating a brand new cluster
  """
  @spec fresh() :: chstate
  def fresh, do: fresh(node())

  def fresh(node_name) do
    fresh(Application.get_env(:onsager_core, :ring_creation_size), node_name)
  end

  @spec fresh(ring_size, term) :: chstate
  def fresh(ring_size, node_name) do
    vclock = VC.increment(node_name, VC.fresh())
    gossip_vsn = Gossip.gossip_version()

    %CHState{
      nodename: node_name,
      clustername: {node_name, :erlang.timestamp()},
      members: [{node_name, {:valid, vclock, [{:gossip_vsn, gossip_vsn}]}}],
      next: [],
      claimant: node_name,
      seen: [{node_name, vclock}],
      rvsn: vclock,
      meta: %MetaEntry{}
    }
  end

  @spec resize(chstate, ring_size) :: chstate
  def resize(state, new_ring_size) do
    new_ring =
      Enum.reduce(all_owners(state), CH.fresh(new_ring_size, :dummyhost_resized), fn {idx, owner},
                                                                                     ring_acc ->
        CH.update(idx, owner, ring_acc)
      end)

    set_chash(state, new_ring)
  end

  @spec get_meta(term, chstate) :: {:ok, term} | :undefined
  def get_meta(key, state) do
    case Map.fetch(key, state.meta) do
      :error ->
        :undefined

      {:ok, :removed} ->
        :undefined

      {:ok, %MetaEntry{value: :removed}} ->
        :undefined

      {:ok, meta} ->
        {:ok, meta.value}
    end
  end

  @spec get_meta(term, term, chstate) :: {:ok, term}
  def get_meta(key, default, state) do
    case get_meta(key, state) do
      :undefined -> {:ok, default}
      result -> result
    end
  end

  @spec get_buckets(chstate) :: [term]
  def get_buckets(state = %CHState{}) do
    Map.keys(state.meta)
    |> Enum.reduce([], fn
      {:bucket, bucket}, acc ->
        [bucket | acc]

      _, acc ->
        acc
    end)
  end

  @doc """
  Return node that owns the given index.
  """
  @spec index_owner(chstate, CH.index_as_int()) :: term
  def index_owner(state, idx) do
    {^idx, owner} = List.keyfind(all_owners(state), idx, 1)
    owner
  end

  def future_owner(state, idx) do
    index_owner(future_ring(state), idx)
  end

  def transfer_node(idx, node, my_state = %CHState{}) do
    case CH.lookup(idx, my_state.chring) do
      ^node ->
        my_state

      _ ->
        my_node_name = my_state.nodename
        vclock = VC.increment(my_node_name, my_state.vclock)
        ch_ring = CH.update(idx, node, my_state.chring)
        %{my_state | vclock: vclock, chring: ch_ring}
    end
  end

  def preflist(key, state = %CHState{}), do: CH.successors(key, state.chring)

  def update_meta(key, val, state) do
    change =
      case Map.fetch(state.meta, key) do
        {:ok, old_meta} -> val / old_meta.value
        :error -> true
      end

    cond do
      change ->
        meta = %MetaEntry{
          lastmod: :calendar.datetime_to_gregorian_seconds(:calendar.universal_time()),
          value: val
        }

        vclock = VC.increment(state.nodename, state.vclock)
        %{state | vclock: vclock, meta: Map.put(state.meta, key, meta)}

      true ->
        state
    end
  end

  def is_resizing(state) do
    case resized_ring(state) do
      :undefined -> false
      {:ok, _} -> true
    end
  end

  def resized_ring(state) do
    case get_meta(:resized_ring, state) do
      {:ok, :cleanup} -> {:ok, state.chring}
      {:ok, chring} -> {:ok, chring}
      _ -> :undefined
    end
  end

  def all_next_owners(state) do
    next = pending_changes(state)
    for {idx, _, next_owner, _, _} <- next, do: {idx, next_owner}
  end

  defp change_owners(state, reassign) do
    Enum.reduce(reassign, state, fn {idx, new_owner}, chstate ->
      try do
        transfer_node(idx, new_owner, chstate)
      rescue
        MatchError -> chstate
      end
    end)
  end

  def pending_changes(state = %CHState{}) do
    state.next
  end

  @doc """
  Return the ring that will exist after pending ownership transfers
  have completed.
  """
  @spec future_ring(chstate) :: chstate
  def future_ring(state) do
    future_ring(state, is_resizing(state))
  end

  def future_ring(state, false) do
    future_state = change_owners(state, all_next_owners(state))
    # leaving = get_members(arg1, arg2)
  end

  defp get_members(members) do
    get_members(members, [:joining, :valid, :leaving, :exiting, :down])
  end

  defp get_members(members, types) do
    for {node, {v, _, _}} <- members, Enum.member?(types, v), do: node
  end
end
