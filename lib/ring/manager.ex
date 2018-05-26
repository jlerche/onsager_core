defmodule OnsagerCore.Ring.Manager do
  use GenServer

  @ring_key :onsager_ring
  @ets :ets_onsager_core_ring_manager
  @promote_timeout 90000

  # start_link
  # get_my_ring

  def get_raw_ring() do
    try do
      ring = :ets.lookup_element(@ets, :raw_ring, 2)
      {:ok, ring}
    catch
      _, _ ->
        GenServer.call(__MODULE__, :get_raw_ring, :infinity)
    end
  end

  # get_raw_ring_chashbin
  # refresh_my_ring

  def refresh_ring(node, cluster_name) do
    GenServer.cast({__MODULE__, node}, {:refresh_my_ring, cluster_name})
  end
end
