defmodule OnsagerCore.Mochiglobal do
  require OnsagerCore.Mochiglobal.Macros

  def get(key) do
    get(key, :undefined)
  end

  def get(key, default) do
    get(key, default, key_to_module(key))
  end

  def get(_key, default, mod) do
    try do
      mod.value
    catch
      :error, :undef ->
        default
    end
  end

  def put(key, value) do
    put(key, value, key_to_module(key))
  end

  def put(_key, val, module) do
    :code.purge(module)

    contents =
      quote do
        def value() do
          unquote(val)
        end
      end

    Module.create(module, contents, Macro.Env.location(__ENV__))
    :ok
  end

  def delete(key) do
    delete(key, key_to_module(key))
  end

  def delete(_key, module) do
    :code.purge(module)
    :code.delete(module)
  end

  def key_to_module(key) do
    :"Elixir.Mochiglobal.#{key}"
  end
end