defmodule OnsagerCore.CHash do
  @type num_partitions :: pos_integer
  @type chash_node :: term
  @type index :: <<_::160>>
  @type index_as_int :: integer
  @type node_entry :: {index_as_int, chash_node}
  @type chash :: {num_partitions, [node_entry]}

  @ringtop trunc(:math.pow(2, 160) - 1)

  @spec contains_name(chash_node, chash) :: boolean
  def contains_name(name, chash) do
    {_num_partitions, nodes} = chash
    [] !== for {_, x} <- nodes, x == name, do: x
  end

  @spec fresh(num_partitions, chash_node) :: chash
  def fresh(num_partitions, seed_node) do
    inc = :ok
  end

  def ring_increment(num_partitions) do
    div(@ringtop, num_partitions)
  end
end
