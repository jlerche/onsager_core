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
  @type index :: integer

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
        _state_a = %CHState{chring: ring_a, meta: meta_a},
        _state_b = %CHState{chring: ring_b, meta: meta_b}
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
      chring: CH.fresh(ring_size, node_name),
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
      {:ok, ring} -> CH.size(ring)
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
    case List.delete(active_members(state), node()) do
      [] -> :no_node
      members_list -> Enum.random(members_list)
    end
  end

  @spec reconcile(chstate, chstate) :: {:no_change | :new_ring, chstate}
  def reconcile(extern_state, my_state) do
    check_tainted(extern_state, "Error: reconciling tainted external ring.")
    check_tainted(my_state, "Error: reconciling tainted internal ring.")

    case internal_reconcile(my_state, extern_state) do
      {false, state} -> {:no_change, state}
      {true, state} -> {:new_ring, state}
    end
  end

  @doc """
  Rename old_node to new_node in the ring
  """
  @spec rename_node(chstate, atom, atom) :: chstate
  def rename_node(
        state = %CHState{
          chring: ring,
          nodename: this_node,
          members: members,
          claimant: claimant,
          seen: seen
        },
        old_node,
        new_node
      )
      when is_atom(old_node)
      when is_atom(new_node) do
    %{
      state
      | chring:
          Enum.reduce(all_owners(state), ring, fn {idx, owner}, acc_in ->
            case owner do
              ^old_node -> CH.update(idx, new_node, acc_in)
              _ -> acc_in
            end
          end),
        members:
          :orddict.from_list(:proplists.substitute_aliases([{old_node, new_node}], members)),
        seen: :orddict.from_list(:proplists.substitute_aliases([{old_node, new_node}], seen)),
        nodename:
          case this_node do
            ^old_node -> new_node
            _ -> this_node
          end,
        claimant:
          case claimant do
            ^old_node -> new_node
            _ -> claimant
          end,
        vclock: VC.increment(new_node, state.vclock)
    }
  end

  @doc """
  Determine the integer ring index responsible for a chash key
  """
  @spec responsible_index(binary, chstate) :: integer
  def responsible_index(chash_key, %CHState{chring: ring}) do
    <<index_as_int::160>> = chash_key
    CH.next_index(index_as_int, ring)
  end

  @doc """
  Given a key and an index in the current ring, determine which index will
  own the key in the future ring. orig_idx may or may not be the responsible index
  for that key (orig_idx may not be the first index in chash_key's preflist). The
  returned index will be in the same position in the preflist for chash_key in the future ring.
  For regular transitions the returned index will always be orig_idx. If the ring
  is resizing the index may be different.
  """
  @spec future_index(CH.index(), integer, chstate) :: integer | :undefined
  def future_index(chash_key, orig_idx, state) do
    future_index(chash_key, orig_idx, :undefined, state)
  end

  @spec future_index(CH.index(), integer, :undefined | integer, chstate) :: integer | :undefined
  def future_index(chash_key, orig_idx, nval_check, state) do
    orig_count = num_partitions(state)
    next_count = future_num_partitions(state)
    future_index(chash_key, orig_idx, nval_check, orig_count, next_count)
  end

  def future_index(chash_key, orig_idx, nval_check, orig_count, next_count) do
    <<chash_int::160>> = chash_key
    orig_inc = CH.ring_increment(orig_count)
    next_inc = CH.ring_increment(next_count)

    # Determine position in the ring of partition that owns the key, ie head of preflist.
    # Position is 1-based starting from partition (0 + ring increment)
    # index 0 is always position N.
    owner_pos = div(chash_int, orig_inc) + 1

    # Determine position of the source partition of the ring
    # if orig_idx is 0 we know that the position is orig_count (number of partitions)
    orig_pos =
      case orig_idx do
        0 -> orig_count
        _ -> div(orig_idx, orig_inc)
      end

    # The distance between the key's owner (head of preflist) and the source partition
    # is the position of the source in the preflist, the distance maybe negative
    # in which case we have wrapped around the ring. distance of zero means the source
    # is the head of the preflist
    orig_dist =
      case orig_pos - owner_pos do
        pos when pos < 0 -> orig_count + pos
        pos -> pos
      end

    # In the case that the ring is shrinking the future index for a key whose position
    # in the preflist is >= ring size may be calculated, any transfer is invalid in
    # this case, return undefined. The position may also be >= an optional N value for
    # the key, if this is true undefined is also returned
    case check_invalid_future_index(orig_dist, next_count, nval_check) do
      true ->
        :undefined

      false ->
        # Determine the partition (head of preflist) that will own the key in the future ring
        future_pos = div(chash_int, next_inc) + 1
        next_owner = future_pos * next_inc

        # Determine the partition that the key should be transferred to (has the same position
        # in future preflist as source partition does in current preflist)
        ring_top = trunc(:math.pow(2, 160) - 1)
        rem(next_owner + next_inc * orig_dist, ring_top)
    end
  end

  def check_invalid_future_index(orig_dist, next_count, nval_check) do
    over_ring_size = orig_dist >= next_count

    over_nval =
      case nval_check do
        :undefined -> false
        _ -> orig_dist >= nval_check
      end

    over_ring_size or over_nval
  end

  @doc """
  Takes the hashed value for the key and any partition, orig_idx,
  in the current preflist for the key. Returns true if target_idx
  is in the same position in the future preflist for that key
  """
  @spec is_future_index(CH.index(), integer, integer, chstate) :: boolean
  def is_future_index(chash_key, orig_idx, target_idx, state) do
    future_index = future_index(chash_key, orig_idx, :undefined, state)
    future_index === target_idx
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

  @doc """
  Return the current claimant
  """
  @spec claimant(chstate) :: node
  def claimant(%CHState{claimant: claimant}) do
    claimant
  end

  def set_claimant(state = %CHState{}, claimant) do
    %{state | claimant: claimant}
  end

  @doc """
  Return the unique identifier for this cluster.
  """
  @spec cluster_name(chstate) :: term
  def cluster_name(state) do
    state.clustername
  end

  @doc """
  Set the unique identifier for this cluster.
  """
  def set_cluster_name(state, name) do
    %{state | clustername: name}
  end

  def reconcile_names(
        ring_a = %CHState{clustername: name_a},
        ring_b = %CHState{clustername: name_b}
      ) do
    case name_a === :undefined || name_b === :undefined do
      true ->
        {%{ring_a | clustername: :undefined}, %{ring_b | clustername: :undefined}}

      false ->
        {ring_a, ring_b}
    end
  end

  def increment_vclock(node, state = %CHState{}) do
    vclock = VC.increment(node, state.vclock)
    %{state | vclock: vclock}
  end

  def ring_version(%CHState{rvsn: rvsn}) do
    rvsn
  end

  def increment_ring_version(node, state) do
    rvsn = VC.increment(node, state.rvsn)
    %{state | rvsn: rvsn}
  end

  @spec member_status(chstate | [node], node) :: member_status
  def member_status(%CHState{members: members}, node) do
    member_status(members, node)
  end

  def member_status(members, node) do
    case :orddict.find(node, members) do
      {:ok, {status, _, _}} -> status
      _ -> :invalid
    end
  end

  @doc """
  Returns the current membership status for all nodes in the cluster.
  """
  @spec all_member_status(chstate) :: [{node, member_status}]
  def all_member_status(%CHState{members: members}) do
    for {node, {status, _vc, _}} <- members, status !== :invalid, do: {node, status}
  end

  def get_member_meta(state, member, key) do
    with {:ok, {_, _, meta}} <- :orddict.find(member, state.members),
         {:ok, value} <- :orddict.find(key, meta) do
      value
    else
      :error -> :undefined
    end
  end

  @doc """
  Set a key in the member metadata orddict
  """
  def update_member_meta(node, state, member, key, val) do
    vclock = VC.increment(node, state.vclock)
    state_2 = update_member_meta(node, state, member, key, val, :same_vclock)
    %{state_2 | vclock: vclock}
  end

  def update_member_meta(node, state, member, key, val, :same_vclock) do
    members = state.members

    case :orddict.is_key(member, members) do
      true ->
        members_2 =
          :orddict.update(
            member,
            fn {status, vclock, meta_data} ->
              {status, VC.increment(node, vclock), :orddict.store(key, val, meta_data)}
            end,
            members
          )

        %{state | members: members_2}

      false ->
        state
    end
  end

  def clear_member_meta(node, state, member) do
    members = state.members

    case :orddict.is_key(member, members) do
      true ->
        members_2 =
          :orddict.update(
            member,
            fn {status, vclock, _meta_dict} ->
              {status, VC.increment(node, vclock), :orddict.new()}
            end,
            members
          )

        %{state | members: members_2}

      false ->
        state
    end
  end

  def add_member(p_node, state, node) do
    set_member(p_node, state, node, :joining)
  end

  def remove_member(p_node, state, node) do
    state_2 = clear_member_meta(p_node, state, node)
    set_member(p_node, state_2, node, :invalid)
  end

  def leave_member(p_node, state, node) do
    set_member(p_node, state, node, :leaving)
  end

  def exit_member(pnode, state, node) do
    set_member(pnode, state, node, :exiting)
  end

  def down_member(p_node, state, node) do
    set_member(p_node, state, node, :down)
  end

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

  @doc """
  Return a list of all members of the  cluster that are eligible to claim partitions
  """
  @spec claiming_members(chstate) :: [node]
  def claiming_members(%CHState{members: members}) do
    get_members(members, [:joining, :valid, :down])
  end

  @doc """
  Return a list of all members of the cluster that are marked down.
  """
  @spec down_members(chstate) :: [node]
  def down_members(%CHState{members: members}) do
    get_members(members, [:down])
  end

  @doc """
  Set the node that is responsible for a given chstate
  """
  @spec set_owner(chstate, node) :: chstate
  def set_owner(state, node) do
    %{state | nodename: node}
  end

  @doc """
  Return all partition indices owned by a node.
  """
  @spec indices(chstate, node) :: [integer]
  def indices(state, node) do
    owners_all = all_owners(state)
    for {idx, owner} <- owners_all, owner === node, do: idx
  end

  @doc """
  Return all partition indices that will be owned by a node after all
  pending ownership transfers have completed.
  """
  @spec future_indices(chstate, node) :: [integer]
  def future_indices(state, node) do
    indices(future_ring(state), node)
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

  @doc """
  Return all indices that a node is scheduled to give to another
  """
  @spec disowning_indices(chstate, node) :: [integer]
  def disowning_indices(state, node) do
    case is_resizing(state) do
      false ->
        for {idx, owner, _next_owner, _mods, _status} <- state.next, owner === node, do: idx

      true ->
        for {idx, owner} <- all_owners(state),
            owner === node,
            disowned_during_resize(state, idx, owner),
            do: idx
    end
  end

  @doc """
  Check if index is being disowned by the current node
  """
  @spec disowned_during_resize(chstate, index, term) :: boolean
  def disowned_during_resize(state, idx, owner) do
    next_owner =
      try do
        future_owner(state, idx)
      catch
        _, _ -> :undefined
      end

    case next_owner do
      ^owner -> false
      _ -> true
    end
  end

  @doc """
  Returns a list of all pending ownership transfers
  """
  def pending_changes(state = %CHState{}) do
    state.next
  end

  def set_pending_changes(state, transfers) do
    %{state | next: transfers}
  end

  @doc """
  Given a ring, resizing, that has been resized (and presumably rebalanced),
  schedule a resize transition for orig.
  """
  @spec set_pending_resize(chstate, chstate) :: chstate
  def set_pending_resize(resizing, orig) do
    # all existing indexes must transfer data when the ring is being resized
    next =
      for {idx, orig_owner} <- all_owners(orig), do: {idx, orig_owner, :resize, [], :awaiting}

    # whether or not the ring is shrinking or expanding, some
    # ownership may be shared between the old and new ring. To prevent
    # degenerate cases where partitions whose ownership does not
    # change are transferred a bunch of data which they in turn must
    # ignore on each subsequent transfer, they move to the front
    # of the next list which is treated as ordered.

    future_owners = all_owners(resizing)

    sorted_next =
      Enum.sort(next, fn {idx, owner, _, _, _}, _ ->
        # only need to check one element
        # true, false -> true
        # true, true -> true
        # false, false -> false
        # false, true -> false
        Enum.member?(future_owners, {idx, owner})
      end)

    # Resizing is assumed to have a modified chring, need to put back
    # the original chring to not install the resized one pre-emptively. The
    # resized ring is stored in ring metadata for later use.
    future_chash = chash(resizing)
    reset_ring = set_chash(resizing, chash(orig))
    set_resized_ring(set_pending_changes(reset_ring, sorted_next), future_chash)
  end

  @spec maybe_abort_resize(chstate) :: {boolean, chstate}
  def maybe_abort_resize(state) do
    resizing = is_resizing(state)
    post_resize = is_post_resize(state)
    pending_abort = is_resize_aborted(state)

    case pending_abort and resizing and not post_resize do
      true ->
        state_1 = %{state | next: []}
        state_2 = clear_all_resize_transfers(state_1)
        state_3 = remove_meta(:resized_ring_abort, state_2)
        {true, remove_meta(:resized_ring, state_3)}

      false ->
        {false, state}
    end
  end

  @spec set_pending_resize_abort(chstate) :: chstate
  def set_pending_resize_abort(state) do
    update_meta(:resized_ring_abort, true, state)
  end

  @spec schedule_resize_transfer(chstate, {integer, term}, integer | {integer, term}) :: chstate
  def schedule_resize_transfer(state, source, target_idx) when is_integer(target_idx) do
    target_node = index_owner(future_ring(state), target_idx)
    schedule_resize_transfer(state, source, {target_idx, target_node})
  end

  def schedule_resize_transfer(state, source, source), do: state

  def schedule_resize_transfer(state, source, target) do
    transfers = resize_transfers(state, source)
    # ignore if we have already scheduled a transferom source to target
    case List.keymember?(transfers, target, 0) do
      true ->
        state

      false ->
        transfers_1 = List.keystore(transfers, target, 0, {target, :ordsets.new(), :awaiting})
        set_resize_transfers(state, source, transfers_1)
    end
  end

  @doc """
  Reassign all outbound and inbound resize transfers from node to new_node.
  """
  @spec reschedule_resize_transfers(chstate, term, term) :: chstate
  def reschedule_resize_transfers(state = %CHState{next: next}, node, new_node) do
    {new_next, new_state} =
      Enum.map_reduce(next, state, fn entry, state_acc ->
        reschedule_resize_operation(node, new_node, entry, state_acc)
      end)

    %{new_state | next: new_next}
  end

  def reschedule_resize_operation(
        curr_node,
        new_node,
        {idx, curr_node, :resize, _mods, _status},
        state
      ) do
    new_entry = {idx, new_node, :resize, :ordsets.new(), :awaiting}
    new_state = reschedule_outbound_resize_transfers(state, idx, curr_node, new_node)
    {new_entry, new_state}
  end

  def reschedule_resize_operation(
        node,
        new_node,
        {idx, other_node, :resize, _mods, _status} = entry,
        state
      ) do
    {changed, new_state} =
      reschedule_inbound_resize_transfers({idx, other_node}, node, new_node, state)

    case changed do
      true ->
        new_entry = {idx, other_node, :resize, :ordsets.new(), :awaiting}
        {new_entry, new_state}

      false ->
        {entry, state}
    end
  end

  def reschedule_inbound_resize_transfers(source, node, new_node, state) do
    func = fn transfer, acc ->
      {new_xfer, new_acc} = reschedule_inbound_resize_transfer(transfer, node, new_node)
      {new_xfer, new_acc or acc}
    end

    {resize_transfers, changed} = Enum.map_reduce(resize_transfers(state, source), false, func)
    {changed, set_resize_transfers(state, source, resize_transfers)}
  end

  def reschedule_inbound_resize_transfer({{idx, target}, _, _}, target, new_node) do
    {{{idx, new_node}, :ordsets.new(), :awaiting}, true}
  end

  def reschedule_inbound_resize_transfer(transfer, _, _) do
    {transfer, false}
  end

  def reschedule_outbound_resize_transfers(state, idx, node, new_node) do
    old_source = {idx, node}
    new_source = {idx, new_node}
    transfers = resize_transfers(state, old_source)

    func = fn
      {i, n} when n === node -> {i, new_node}
      t -> t
    end

    new_transfers =
      for {target, _, _} <- transfers, do: {func.(target), :ordsets.new(), :awaiting}

    set_resize_transfers(clear_resize_transfers(old_source, state), new_source, new_transfers)
  end

  @doc """
  Returns the first awaiting resize_transfer for a {source_idx, source_node} pair.
  If all transfers for the pair are complete, undefined is returned.
  """
  @spec awaiting_resize_transfer(chstate, {integer, term}, module) :: {integer, term} | :undefined
  def awaiting_resize_transfer(state, source, mod) do
    resize_transfers = resize_transfers(state, source)

    awaiting =
      for {target, mods, status} <- resize_transfers,
          status !== :complete,
          not :ordsets.is_element(mod, mods),
          do: {target, mods, status}

    case awaiting do
      [] -> :undefined
      [{target, _, _}, _] -> target
    end
  end

  @doc """
  Return the status of a resize_transfer for source (index-node pair). :undefined
  is returned if no such transfer is scheduled. :complete is returned if the transfer
  is marked as such or mod is contained in the completed modules set. :awaiting is returned
  otherwise.
  """
  @spec resize_transfer_status(chstate, {integer, term}, {integer, term}, module) ::
          :awaiting | :complete | :undefined
  def resize_transfer_status(state, source, target, mod) do
    resize_transfers = resize_transfers(state, source)

    is_complete =
      case List.keyfind(resize_transfers, target, 0) do
        false -> :undefined
        {^target, _, :complete} -> true
        {^target, mods, :awaiting} -> :ordsets.is_element(mod, mods)
      end

    case is_complete do
      true -> :complete
      false -> :awaiting
      :undefined -> :undefined
    end
  end

  def resize_transfer_complete(state, source = {src_idx, _}, target, mod) do
    resize_transfers = resize_transfers(state, source)
    transfer = List.keyfind(resize_transfers, target, 0)

    case transfer do
      {^target, mods, status} ->
        vnode_mods = :ordsets.from_list(for {_, vmod} <- OnsagerCore.vnode_modules(), do: vmod)
        mods_2 = :ordsets.add_element(mod, mods)

        status_2 =
          case {status, mods_2} do
            {:complete, _} ->
              :complete

            {:awaiting, ^vnode_mods} ->
              :complete

            _ ->
              :awaiting
          end

        resize_transfers_2 =
          List.keyreplace(resize_transfers, target, 0, {target, mods_2, status_2})

        state_1 = set_resize_transfers(state, source, resize_transfers_2)

        all_complete =
          Enum.all?(resize_transfers_2, fn
            {_, _, :complete} -> true
            {_, mods_3, :awaiting} -> :ordsets.is_element(mod, mods_3)
          end)

        case all_complete do
          true -> transfer_complete(state_1, src_idx, mod)
          false -> state_1
        end

      _ ->
        state
    end
  end

  @spec is_resizing(chstate) :: boolean
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

  def is_resize_aborted(state) do
    case get_meta(:resized_ring_abort, state) do
      {:ok, true} -> true
      _ -> false
    end
  end

  @spec is_resize_complete(chstate) :: boolean
  def is_resize_complete(%CHState{next: next}) do
    not Enum.any?(next, fn
      {_, _, _, _, :awaiting} -> true
      {_, _, _, _, :complete} -> false
    end)
  end

  @spec complete_resize_transfers(chstate, {integer, term}, module) :: [{integer, term}]
  def complete_resize_transfers(state, source, mod) do
    for {target, mods, status} <- resize_transfers(state, source),
        status === :complete or :ordsets.is_element(mod, mods),
        do: target
  end

  @spec deletion_complete(chstate, integer, module) :: chstate
  def deletion_complete(state, idx, mod) do
    transfer_complete(state, idx, mod)
  end

  @spec resize_transfers(chstate, {integer, term}) :: [resize_transfer]
  def resize_transfers(state, source) do
    {:ok, transfers} = get_meta({:resize, source}, [], state)
    transfers
  end

  @spec set_resize_transfers(chstate, {integer, term}, [resize_transfer]) :: chstate
  def set_resize_transfers(state, source, transfers) do
    update_meta({:resize, source}, transfers, state)
  end

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

  def completed_next_owners(mod, %CHState{next: next}) do
    for ninfo = {idx, _, _, _, _} <- next,
        {owner, next_owner, :complete} <- [next_owner_status(ninfo, mod)],
        do: {idx, owner, next_owner}
  end

  @doc """
  Returns true if all cluster members have seen the current ring.
  """
  @spec ring_ready(chstate) :: boolean
  def ring_ready(state) do
    check_tainted(state, "Error: ring_ready called on tainted ring")
    owner = owner_node(state)
    state_1 = update_seen(owner, state)
    seen = state_1.seen
    members = get_members(state_1.members, [:valid, :leaving, :exiting])
    vclock = state_1.vclock

    ring =
      for node <- members do
        case :orddict.find(node, seen) do
          :error ->
            false

          {:ok, vec_clock} ->
            VC.equal(vclock, vec_clock)
        end
      end

    ready = Enum.all?(ring, fn x -> x === true end)
    ready
  end

  def ring_ready() do
    {:ok, ring} = OnsagerCore.Ring.Manager.get_raw_ring()
    ring_ready(ring)
  end

  def ring_ready_info(state_0) do
    owner = owner_node(state_0)
    state = update_seen(owner, state_0)
    seen = state.seen
    members = get_members(state.members, [:valid, :leaving, :exiting])

    recent_vclock =
      :orddict.fold(
        fn _, vc, recent ->
          case VC.descends(vc, recent) do
            true -> vc
            false -> recent
          end
        end,
        state.vclock,
        seen
      )

    outdated =
      :orddict.filter(
        fn node, vc -> not VC.equal(vc, recent_vclock) and Enum.member?(members, node) end,
        seen
      )

    outdated
  end

  @doc """
  Marks a pending transfer as complete
  """
  @spec handoff_complete(chstate, integer, module) :: chstate
  def handoff_complete(state, idx, mod) do
    transfer_complete(state, idx, mod)
  end

  def ring_changed(node, state) do
    check_tainted(state, "Error ring_changed called on tainted ring")
    internal_ring_changed(node, state)
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

  defp internal_ring_changed(node, cstate) do
    state = update_seen(node, cstate)

    case ring_ready(state) do
      false -> state
      true -> OnsagerCore.Claim.Claimant.ring_changed(node, state)
    end
  end

  # merge_meta
  # pick_val
  # log_meta_merge
  # log_ring_result

  def internal_reconcile(_state, _other_state) do
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

  defp transfer_complete(cstate = %CHState{next: next, vclock: vclock}, idx, mod) do
    {idx, owner, next_owner, transfers, status} = List.keyfind(next, idx, 0)
    transfers_2 = :ordsets.add_element(mod, transfers)
    vnode_mods = :ordsets.from_list(for {_, vmod} <- OnsagerCore.vnode_modules(), do: vmod)

    status_2 =
      case {status, transfers} do
        {:complete, _} -> :complete
        {:awaiting, ^vnode_mods} -> :complete
        _ -> :awaiting
      end

    next_2 = List.keyreplace(next, idx, 0, {idx, owner, next_owner, transfers_2, status_2})
    vclock_2 = VC.increment(owner, vclock)
    %{cstate | next: next_2, vclock: vclock_2}
  end

  defp get_members(members) do
    get_members(members, [:joining, :valid, :leaving, :exiting, :down])
  end

  defp get_members(members, types) do
    for {node, {v, _, _}} <- members, Enum.member?(types, v), do: node
  end

  def update_seen(node, state = %CHState{vclock: vclock, seen: seen}) do
    seen_2 = :orddict.update(node, fn seen_vc -> VC.merge([seen_vc, vclock]) end, vclock, seen)
    %{state | seen: seen_2}
  end

  # equal_cstate
  # equal_members
  # equal_seen
  # filtered_seen
end
