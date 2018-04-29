defmodule OnsagerCore.Ring do
  require Record

  Record.defrecord(:chstate, [
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
  ])

  Record.defrecord(:meta_entry, [
    :value,
    :lastmod
  ])

  def set_tainted(ring) do
    update_meta(:onsager_core_ring_tainted, true, ring)
  end

  def update_meta(key, val, state) do
    change =
      case Map.fetch(chstate(state, :meta), key) do
        {:ok, old_meta} -> val / meta_entry(old_meta, :value)
        :error -> true
      end

    case change do
      true ->
        state

      _ ->
        # TODO finish
        meta = nil
    end
  end
end
