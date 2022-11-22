defmodule HTest do
  use ExUnit.Case
  doctest H

  test "greets the world" do
    assert H.hello() == :world
  end
end
