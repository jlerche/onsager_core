defmodule OnsagerCore.Ring.Manager do
  use GenServer

  @ring_key :onsager_ring
  @ets :ets_onsager_core_ring_manager
end
