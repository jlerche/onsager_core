defmodule OnsagerCore.Ring do
  defmodule OnsagerCore.Ring.CHState do
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
  end

  defmodule OnsagerCore.Ring.MetaEntry do
    defstruct [:value, :lastmod]
  end

  def set_tainted(ring) do
    update_meta(:onsager_core_ring_tainted, true, ring)
  end

  def update_meta(key, val, state) do
    change =
      case Map.fetch(state.meta, key) do
        {:ok, old_meta} -> val / old_meta.value
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
