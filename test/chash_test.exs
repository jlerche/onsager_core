defmodule OnsagerCore.CHashTest do
  use ExUnit.Case
  alias OnsagerCore.CHash

  test "successors length" do
    assert 8 == length(CHash.successors(CHash.key_of(0), CHash.fresh(8, :the_node)))
  end
end
