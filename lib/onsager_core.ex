defmodule OnsagerCore do
  alias OnsagerCore.{Util}

  @wait_print_interval 60 * 1000
  @wait_poll_interval 100

  def stop() do
    stop("riak stop requested")
  end

  def stop(_reason) do
    # log reason
    # possibly overkill vs Application.stop/1
    :init.stop()
  end

  def join(node) do
    join(node, true)
  end

  # staged join?

  def join(node_str, auto) when is_list(node_str) do
    join(Util.str_to_node(node_str), auto)
  end

  def join(node, auto) when is_atom(node) do
    join(node(), node, auto)
  end

  def join(node, node, _) do
    {:error, :self_join}
  end

  def join(_, node, auto) do
    join(node(), node, false, auto)
  end

  def join(_, node, rejoin, auto) do
    case :net_adm.ping(node) do
      :pang ->
        {:error, :not_reachable}

      :pong ->
        standard_join(node, rejoin, auto)
    end
  end

  defp get_other_ring(node) do
    Util.safe_rpc(node, :onsager_core_ring_manager, :get_raw_ring, [])
  end

  def standard_join(node, rejoin, auto) when is_atom(node) do
    with :pong <- :net_adm.ping(node),
         {:ok, ring} <- get_other_ring(node) do
      standard_join(node, ring, rejoin, auto)
    else
      :pang ->
        {:error, :not_reachable}

      _ ->
        {:error, :unable_to_get_join_ring}
    end
  end

  def standard_join(node, ring, rejoin, auto) do
    #
  end
end
