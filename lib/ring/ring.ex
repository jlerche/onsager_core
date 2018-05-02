defmodule OnsagerCore.Ring do
  @moduledoc """
  Manages a node's local view of partition ownership. State is encapsulated
  in the CHState struct (Consistent Hashing Struct) and exchanged between nodes
  via a gossip protocol.
  """

  alias OnsagerCore.VectorClock, as: VC

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
            chring: term,
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

  def nearly_equal(ring_a, ring_b) do
    test_vc = VC.descends(ring_b.vclock, ring_a.vclock)
    ring_a2 = %{ring_a | vclock: :undefined, meta: :undefined}
    ring_b2 = %{ring_b | vclock: :undefined, meta: :undefined}
    test_ring = Map.equal?(ring_a2, ring_b2)
    test_vc && test_ring
  end

  def is_primary(ring, idx_node) do
    owners = all_owners(ring)
    Enum.member?(owners, idx_node)
  end

  def all_owners(state) do
    []
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
end
