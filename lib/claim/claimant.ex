defmodule OnsagerCore.Claim.Claimant do
  # start_link
  # plan
  # commit
  # leave_member
  # remove member
  # replace
  # force_replace
  # resize_ring
  # abort_resize
  # clear
  def ring_changed(node, ring) do
    internal_ring_changed(node, ring)
  end

  # create_bucket_type
  # activate_bucket_type
  # get_bucket_type
  # get_bucket_type
  # update_bucket_type
  # bucket_type_iterator
  # reassign_indices

  # stage
  # claimant
  # maybe_filter_inactive_type
  # maybe_filter_inactive_type

  # init
  # handle_call
  # handle_call
  # handle_call x2342
  # handle cast x2313123
  # handle_info x4
  # terminate
  # code_change

  # maybe_stage
  # generate_plan
  # generate_plan
  # generate_plan
  # commit_staged
  # commit_staged
  # maybe_commit_staged
  # maybe_commit_staged
  # maybe_commit_staged
  # clear_staged
  # remove_joining_nodes
  # remove_joining_nodes
  # remove_joining_nodes,_from_ring
  # valid_request
  # valid_leave_request
  # valid_remove_request
  # valid_replace_request
  # valid_force_replace_request
  # valid_resize_request
  # valid_resize_abort_request
  # filter_changes
  # filter_changes_pred
  # filter_changes_pred
  # existing_replacements
  # same_plan
  # schedule_tick
  # tick
  # maybe_force_ring_update
  # do_maybe_force_ring_update
  # can_create_type
  # can_create_type
  # can_update_type
  # can_update_type
  # get_type_status
  # get_type_status
  # get_remote_type_status
  # maybe_activate_type
  # maybe_activate_type
  # maybe_activate_type
  # maybe_activate_type
  # type_active
  # type_claimant
  # maybe_enable_ensembles
  # enable_ensembles
  # ensemble_singleton
  # become_ensemble_singleton
  # become_ensemble_singleton_trans
  # maybe_bootstrap_root_ensemble
  # bootstrap_root_ensemble
  # bootstrap_members
  # async_bootstrap_members
  # maybe_reset_ring_id
  # reset_ring_id
  # compute_all_next_rings
  # compute_all_next_rings
  # compute_next_ring
  # maybe_compute_resize
  # compute_resize
  # schedule_first_resize_transfer
  # schedule_first_resize_transfer
  # schedule_first_resize_transfer
  # validate_resized_ring
  # apply_changes
  # change
  # change
  # change
  # change
  # change
  # change
  # change

  def internal_ring_changed(_node, cstate) do
    # temp placeholder
    cstate
  end

  def inform_removed_nodes(node, old_ring, new_ring) do
    cname = OnsagerCore.Ring.cluster_name(new_ring)
    exiting = OnsagerCore.Ring.members(old_ring, [:exiting]) -- [node]
    invalid = OnsagerCore.Ring.members(new_ring, [:invalid])
    changed = :ordsets.intersection(:ordsets.from_list(exiting), :ordsets.from_list(invalid))

    # tell exiting node to shutdown
    Enum.each(changed, fn exiting_node ->
      OnsagerCore.Ring.Manager.refresh_ring(exiting_node, cname)
    end)
  end

  # do_claimant_quiet
  # do_claimant
  # do_claimant
  # maybe_update_claimant
  # maybe_update_ring
  # maybe_remove_exiting
  # are_joining_nodes
  # auto_joining_nodes
  # maybe_handle_auto_joining
  # maybe_handle_joining
  # maybe_handle_joining
  # update_ring
  # update_ring
  # maybe_install_resized_ring
  # transfer_ownership
  # reassign_indices
  # rebalance_ring
  # rebalance_ring
  # rebalance_ring
  # handle_down_nodes
  # reassign_indices_to
  # remove_node
  # remove_node
  # remove_node
  # replace_node_during_resize
  # replace_node_during_resize
  # replace_node_during_resize
  # no_log
  # log
  # log
  # log
  # log
  # log
end
