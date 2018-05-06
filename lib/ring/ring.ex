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
    %CHState{}
  end

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

  defp get_members(members) do
    get_members(members, [:joining, :valid, :leaving, :exiting, :down])
  end

  defp get_members(members, types) do
    for {node, {v, _, _}} <- members, Enum.member?(types, v), do: node
  end
end
