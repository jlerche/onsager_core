defmodule OnsagerCore.Ring do
  require Record

  Record.defrecord(:chstate, [
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
  ])
end
