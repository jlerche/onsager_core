defmodule OnsagerCore do
  alias OnsagerCore.{Util}
  alias OnsagerCore.Ring

  @wait_print_interval 60 * 1000
  @wait_poll_interval 100

  def stop() do
    stop("onsager stop requested")
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

  def get_other_ring(node) do
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

  defp init_complete({:started, _}) do
    true
  end

  defp init_complete(_) do
    false
  end

  def standard_join(node, ring, rejoin, auto) do
    {:ok, my_ring} = Ring.Manager.get_raw_ring()
    init_complete = init_complete(:init.get_status())
    same_size = Ring.num_partitions(my_ring) === Ring.num_partitions(ring)
    singleton = [node()] === Ring.all_members(my_ring)
  end

  # maybe_auto_join
  # maybe_auto_join
  # remove
  # standard_remove
  # down
  # leave
  # standard_leave
  # remove_from_cluster

  def vnode_modules() do
    case Application.get_env(:onsager_core, :vnode_modules) do
      :undefined -> []
      {:ok, mods} -> mods
    end
  end

  # bucket_fixups
  # bucket_validators
  # stat_mods
  # health_check
  # get_app
  # register
  # register
  # register
  # register
  # register
  # register
  # register
  # register
  # register
  # register
  # register_mod
  # register_metadata
  # register_proplist
  # add_guarded_event_handler
  # add_guarded_event_handler
  # delete_guarded_event_handler
  # app_for_module
  # app_for_module
  # app_for_module
  # wait_for_application
  # wait_for_application
  # wait_for_service
  # wait_for_service
end
