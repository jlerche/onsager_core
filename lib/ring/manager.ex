defmodule OnsagerCore.Ring.Manager do
  use GenServer

  defmodule State do
    defstruct [
      :mode,
      :raw_ring,
      :ring_changed_time,
      :inactivity_timer
    ]
  end

  @ring_key :onsager_ring
  @ets :ets_onsager_core_ring_manager
  @promote_timeout 90000

  # start_link
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

  # set_my_ring
  # get_ring_id
  # get_bucket_meta
  # get_bucket_meta
  # get_bucket_meta
  # get_chash_bin
  # write_ringfile
  # ring_trans
  # set_cluster_name
  # is_stable_ring
  # force_update
  # do_write_ringfile
  # do_write_ringfile
  # find_latest_ringfile
  # read_ringfile
  # prune_ringfiles

  # init
  # reload_ring
  # reload_ring
  # handle_call
  # handle_call
  # handle_call
  # handle_call
  # handle_call
  # handle_call
  # handle_call
  # handle_call
  # handle_cast
  # handle_cast
  # handle_cast
  # handle_cast
  # handle_info
  # handle_info
  # terminate
  # code_change

  # ring_dir
  # prune_list
  # back
  # back

  # run_fixups
  # run_fixups
  # set_ring
  # maybe_set_timer
  # maybe_set_timer
  # set_timer
  # setup_ets
  # cleanup_ets
  # reset_ring_id
  # set_ring_global
  # promote_ring
  # prune_write_notify_ring
  # prune_write_ring
  # is_stable_ring
end
