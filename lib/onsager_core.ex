defmodule OnsagerCore do
  @wait_print_interval 60 * 1000
  @wait_poll_interval 100

  def stop() do
    stop("riak stop requested")
  end

  def stop(_reason) do
    # lager:notice("~p", [reason])
    :init.stop()
  end
end
