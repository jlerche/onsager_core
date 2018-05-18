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

  @doc """
  Return all partition indices owned by the node executing this function
  """
  @spec my_indices(chstate) :: CH.index_as_int()
  def my_indices(state) do
    for {idx, owner} <- all_owners(state), owner === node(), do: idx
  end

  @doc """
  Return the number of partitions in this ring
  """
  @spec num_partitions(chstate) :: pos_integer
  def num_partitions(state) do
    CH.size(state.chring)
  end

  @spec future_num_partitions(chstate) :: pos_integer
  def future_num_partitions(state = %CHState{chring: chring}) do
    case resized_ring(state) do
      {:ok, ring} -> CH.size(chring)
      :undefined -> CH.size(chring)
    end
  end

  @doc """
  Return the node responsible for the chstate
  """
  @spec owner_node(chstate) :: node
  def owner_node(state = %CHState{}), do: state.nodename

  @doc """
  Gives the ordered list {partition, node} elements responsible for a given object key
  """
  @spec preflist(binary, chstate) :: [{CH.index_as_int(), term}]
  def preflist(key, state = %CHState{}), do: CH.successors(key, state.chring)

  @doc """
  Return randomly chosen node from the owners
  """
  @spec random_node(chstate) :: node
  def random_node(state) do
    members = all_members(state)
    Enum.random(members)
  end

  @doc """
  Return partition index not owned by the node executing this function. If node
  owns all partitions, return any index
  """
  @spec random_other_index(chstate) :: CH.index_as_int()
  def random_other_index(state) do
    all_owners_list = for {idx, owner} <- all_owners(state), owner !== node(), do: idx

    case all_owners_list do
      [] -> hd(my_indices(state))
      _ -> Enum.random(all_owners_list)
    end
  end

  @spec random_other_index(chstate, [term]) :: CH.index_as_int() | :no_indices
  def random_other_index(state, exclude) when is_list(exclude) do
    all_owners_list =
      for {idx, owner} <- all_owners(state),
          owner !== node(),
          not Enum.member?(exclude, idx),
          do: idx

    case all_owners_list do
      [] -> :no_indices
      all_owners_list -> Enum.random(all_owners_list)
    end
  end

  @doc """
  Return a random node from owners other than this one
  """
  @spec random_other_node(chstate) :: node | :no_node
  def random_other_node(state) do
    case List.delete(all_members(state), node()) do
      [] -> :no_node
      all_members_list -> Enum.random(all_members_list)
    end
  end

  @doc """
  Return random active node other than this one.
  """
  @spec random_other_active_node(chstate) :: node | :no_node
  def random_other_active_node(state) do
    case List.delete(active_members(state), node) do
      [] -> :no_node
      members_list -> Enum.random(members_list)
    end
  end

  def reconcile(extern_state, my_state) do
    check_tainted(extern_state, "Error: reconciling tainted external ring.")
    check_tainted(my_state, "Error: reconciling tainted internal ring.")

    case internal_reconcile(my_state, extern_state) do
      {false, state} -> {:no_change, state}
      {true, state} -> {:new_ring, state}
    end
  end

  # rename_node
  # responsible_index
  # future_index
  # check_invalid_future_index
  # is_future_index

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

  @doc """
  Delete key in cluster metadata Map
  """
  @spec remove_meta(term, chstate) :: chstate
  def remove_meta(key, state) do
    case Map.fetch(state.meta, key) do
      {:ok, _} -> update_meta(key, :removed, state)
      :error -> state
    end
  end

  # claimant
  # set_claimant
  # cluster_name
  # reconcile_names
  # increment_vclock
  # ring_version
  # increment_ring_version
  # member_status
  # all_member_status
  # get_member_meta
  # update_member_meta
  # clear_member_meta
  # add_member
  # remove_member
  # leave_member

  def exit_member(pnode, state, node) do
    set_member(pnode, state, node, :exiting)
  end

  # down_member

  def set_member(node, chstate, member, status) do
    vclock = VC.increment(node, chstate.vclock)
    chstate_2 = set_member(node, chstate, member, status, :same_vclock)
    %{chstate_2 | vclock: vclock}
  end

  def set_member(node, chstate, member, status, :same_vclock) do
    members_2 =
      :orddict.update(
        member,
        fn {_, vc, md} -> {status, VC.increment(node, vc), md} end,
        {status, VC.increment(node, VC.fresh()), []},
        chstate.members
      )

    %{chstate | members: members_2}
  end

  # claiming_members
  # down_members
  # set_owner

  def indices(state, node) do
    owners_all = all_owners(state)
    for {idx, owner} <- owners_all, owner === node, do: idx
  end

  # future_indices

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

  # disowning_indices
  # disowned_during_resize

  def pending_changes(state = %CHState{}) do
    state.next
  end

  # set_pending_changes
  # set_pending_resize
  # maybe_abort_resize
  # set_pending_resize_abort
  # schedule resize_transfer
  # reschedule_resize_transfer
  # reschedule_resize_transfers
  # reschedule_resize_operation
  # reschedule_inbound_resize_transfers
  # reschedule_inbound_resize_transfer
  # reschedule_outbound_resize_transfers
  # awaiting_resize_transfer
  # resize_transfer_status
  # resize_transfer_complete

  def is_resizing(state) do
    case resized_ring(state) do
      :undefined -> false
      {:ok, _} -> true
    end
  end

  def is_post_resize(state) do
    case get_meta(:resized_ring, state) do
      {:ok, :cleanup} -> true
      _ -> false
    end
  end

  # is_resize_aborted
  # is_resize_complete
  # complete_resize_transfers
  # deletion_complete
  # resize_transfers
  # set_resize_transfers

  def clear_all_resize_transfers(state) do
    Enum.reduce(all_owners(state), state, &clear_resize_transfers/2)
  end

  def clear_resize_transfers(source, state) do
    remove_meta({:resize, source}, state)
  end

  @spec resized_ring(chstate) :: {:ok, CH.chash()} | :undefined
  def resized_ring(state) do
    case get_meta(:resized_ring, state) do
      {:ok, :cleanup} -> {:ok, state.chring}
      {:ok, chring} -> {:ok, chring}
      _ -> :undefined
    end
  end

  @spec set_resized_ring(chstate, CH.chash()) :: chstate
  def set_resized_ring(state, future_chash) do
    update_meta(:resized_ring, future_chash, state)
  end

  def cleanup_after_resize(state) do
    update_meta(:resized_ring, :cleanup, state)
  end

  @spec vnode_type(chstate, integer) ::
          :primary | {:fallback, term} | :future_primary | :resized_primary
  def vnode_type(state, idx) do
    vnode_type(state, idx, node())
  end

  def vnode_type(state, idx, node_name) do
    try do
      case index_owner(state, idx) do
        ^node_name ->
          :primary

        owner ->
          case next_owner(state, idx) do
            {_, ^node_name, _} -> :future_primary
            _ -> {:fallback, owner}
          end
      end
    catch
      :error, {:badmatch, _} -> :resized_primary
    end
  end

  def next_owner(state, idx) do
    case List.keyfind(state.next, idx, 0) do
      false ->
        {:undefined, :undefined, :undefined}

      next_info ->
        next_owner(next_info)
    end
  end

  def next_owner(state, idx, module) do
    next_info = List.keyfind(state.next, idx, 0)
    next_owner_status(next_info, module)
  end

  def next_owner_status(next_info, module) do
    case next_info do
      false ->
        {:undefined, :undefined, :undefined}

      {_, owner, next_owner, _transfers, :complete} ->
        {owner, next_owner, :complete}

      {_, owner, next_owner, transfers, _status} ->
        case :ordsets.is_element(module, transfers) do
          true ->
            {owner, next_owner, :complete}

          false ->
            {owner, next_owner, :awaiting}
        end
    end
  end

  defp next_owner({_, owner, next_owner, _transfers, status}) do
    {owner, next_owner, status}
  end

  # completed_next_owners
  # ring_ready
  # ring_ready_info
  # handoff_complete
  # ring_changed

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
    leaving = get_members(future_state.members, [:leaving])

    future_state_2 =
      Enum.reduce(leaving, future_state, fn node, state_acc ->
        case indices(state_acc, node) do
          [] -> exit_member(node, state_acc, node)
          _ -> state_acc
        end
      end)

    %{future_state_2 | next: []}
  end

  def future_ring(state_0 = %CHState{next: old_next}, true) do
    case is_post_resize(state_0) do
      false ->
        {:ok, future_chash} = resized_ring(state_0)
        state1 = cleanup_after_resize(state_0)
        state2 = clear_all_resize_transfers(state1)
        resized = %{state2 | chring: future_chash}

        next =
          Enum.reduce(old_next, [], fn {idx, owner, :resize, _, _}, acc ->
            delete_entry = {idx, owner, :delete, [], :awaiting}

            try do
              case index_owner(resized, idx) do
                ^owner -> acc
                _ -> [delete_entry | acc]
              end
            catch
              :error, {:badmatch, _} -> [delete_entry | acc]
            end
          end)

        %{resized | next: next}

      true ->
        state1 = remove_meta(:resized_ring, state_0)
        %{state1 | next: []}
    end
  end

  # pretty_print
  # cancel_transfers

  # random legacy stuff

  # internal_ring_changed
  # merge_meta
  # pick_val
  # log_meta_merge
  # log_ring_result

  def internal_reconcile(state, other_state) do
  end

  # reconcile_divergent
  # reconcile_members
  # reconcile_seen
  # merge_next_status
  # reconcile_next
  # reconcile_divergent_next
  # substitute
  # reconcile_ring
  # merge_status x10
  # transfer_complete

  defp get_members(members) do
    get_members(members, [:joining, :valid, :leaving, :exiting, :down])
  end

  defp get_members(members, types) do
    for {node, {v, _, _}} <- members, Enum.member?(types, v), do: node
  end

  # update_seen
  # equal_cstate
  # equal_members
  # equal_seen
  # filtered_seen
end
