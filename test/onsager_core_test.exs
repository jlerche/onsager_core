defmodule OnsagerCoreTest do
  use ExUnit.Case
  doctest OnsagerCore

  test "greets the world" do
    assert OnsagerCore.hello() == :world
  end
end
