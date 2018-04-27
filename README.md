# onsager_core

Really, mostly a port of riak_core but using server wide clocks instead of merkle trees as the active anti-entropy method. Mainly a learning experience but who knows might become an actual production ready library :dancer:.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `onsager_core` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:onsager_core, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/onsager_core](https://hexdocs.pm/onsager_core).
