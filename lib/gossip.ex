defmodule OnsagerCore.Gossip do
  @moduledoc """
  Gossip protocol shuttles data from one node to another.

  Also checks that current node has its fair share of partitions, sends
  copy of the ring to other random node for synchronization.
  """

  @legacy_ring_vsn 1
  @current_ring_vsn 2

  def gossip_version() do
    @current_ring_vsn
  end
end
