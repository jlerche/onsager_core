defmodule OnsagerCore.Util do
  @sec_to_epoch 62_167_219_200

  def moment do
    {mega, sec, _micro} = :os.timestamp()
    mega * 1_000_000 + sec + @sec_to_epoch
  end

  def compare_dates(a = {_, _, _}, b = {_, _, _}) do
    # assume 3-tuples are :erlang.now() times
    a > b
  end

  def compare_dates(a, b) when is_list(a) do
    #
  end

  defp rfc1123_to_now(string) when is_list(string) do
    g_sec = :calendar.datetime_to_gregorian_seconds(:httpd_util.convert_request_date(string))
    e_sec = g_sec - @sec_to_epoch
    sec = rem(e_sec, 1_000_000)
    m_sec = div(e_sec, 1_000_000)
    {m_sec, sec, 0}
  end

  # replace_file
  # read_file
  # integer_to_list
  # unique_id_62

  def str_to_node(node) when is_atom(node) do
    str_to_node(Atom.to_string(node))
  end

  def str_to_node(node_str) do
    case String.split(node_str, "@") do
      [node_name] ->
        case node_hostname() do
          [] ->
            String.to_atom(node_name)

          host_name ->
            String.to_atom(node_name ++ "@" ++ host_name)
        end

      _ ->
        String.to_atom(node_str)
    end
  end

  defp node_hostname do
    node_str = Atom.to_string(node())

    case String.split(node_str, "@") do
      [_node_name, host_name] ->
        host_name

      _ ->
        []
    end
  end

  # start_app_deps
  # count
  # keydelete
  # multi_keydelete
  # compose
  # pmap

  # in case rex is down
  def safe_rpc(node, module, function, args) do
    try do
      :rpc.call(node, module, function, args)
    catch
      :exit, {:noproc, _details} -> {:badrpc, :rpc_process_down}
    end
  end

  def safe_rpc(node, module, function, args, timeout) do
    try do
      :rpc.call(node, module, function, args, timeout)
    catch
      :exit, {:noproc, _detauls} -> {:badrpc, :rpc_process_down}
    end
  end
end
