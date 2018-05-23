defmodule OnsagerCore.CHash do
  @moduledoc """
  Consistent hashing implementation
  """
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
    inc = ring_increment(num_partitions)

    {num_partitions,
     for(index_as_int <- :lists.seq(0, @ringtop - 1, inc), do: {index_as_int, seed_node})}
  end

  @spec lookup(index_as_int, chash) :: chash_node
  def lookup(index_as_int, chash) do
    {_num_partitions, nodes} = chash
    # gotta use proplists unless constrain chash_node to atom
    {^index_as_int, x} = :proplists.lookup(index_as_int, nodes)
    x
  end

  defp sha(binary) do
    :crypto.hash(:sha, binary)
  end

  @doc """
  A pre-processing step to convert terms to binary before hashing
  """
  @spec key_of(term) :: index
  def key_of(object_name) do
    sha(:erlang.term_to_binary(object_name))
  end

  @doc """
  Returns nodes that own partitions in the ring
  """
  @spec members(chash) :: [chash_node]
  def members(chash) do
    {_num_partitions, nodes} = chash
    :lists.usort(for {_idx, x} <- nodes, do: x)
  end

  @doc """
  Returns randomized merge of two ring. Churn will occur
  if multiple nodes are claiming the same nodes.
  """
  @spec merge_rings(chash, chash) :: chash
  def merge_rings(chash_a, chash_b) do
    {num_partitions, nodes_a} = chash_a
    {^num_partitions, nodes_b} = chash_b

    {num_partitions,
     for({{idx, a}, {idx, b}} <- Enum.zip(nodes_a, nodes_b), do: {idx, random_node(a, b)})}
  end

  @spec next_index(integer, chash) :: index_as_int
  def next_index(integer_key, {num_partitions, _}) do
    inc = ring_increment(num_partitions)

    integer_key
    |> div(inc)
    |> (&(&1 + 1)).()
    |> rem(num_partitions)
    |> (&(&1 * inc)).()
  end

  @spec nodes(chash) :: [node_entry]
  def nodes(chash) do
    {_num_partitions, nodes} = chash
    nodes
  end

  @spec ordered_from(index, chash) :: [node_entry]
  def ordered_from(index, {num_partitions, nodes}) do
    <<index_as_int::160>> = index
    inc = ring_increment(num_partitions)
    {a, b} = Enum.split(nodes, div(index_as_int, inc) + 1)
    b ++ a
  end

  @spec predecessors(index | index_as_int, chash) :: [node_entry]
  def predecessors(index, chash) do
    {num_partitions, _nodes} = chash
    predecessors(index, chash, num_partitions)
  end

  @spec predecessors(index | index_as_int, chash, integer) :: integer
  def predecessors(index, chash, num) when is_integer(index) do
    predecessors(<<index::160>>, chash, num)
  end

  def predecessors(index, chash, n) do
    num = max_n(n, chash)
    {result, _} = Enum.split(Enum.reverse(ordered_from(index, chash)), num)
    result
  end

  @spec ring_increment(pos_integer) :: pos_integer
  def ring_increment(num_partitions) do
    div(@ringtop, num_partitions)
  end

  @spec size(chash) :: integer
  def size(chash) do
    {_num_partitions, nodes} = chash
    length(nodes)
  end

  @spec successors(index, chash) :: [node_entry]
  def successors(index, chash) do
    {num_partitions, _nodes} = chash
    successors(index, chash, num_partitions)
  end

  @spec successors(index, chash, integer) :: [node_entry]
  def successors(index, chash, n) do
    num = max_n(n, chash)
    ordered = ordered_from(index, chash)
    {num_partitions, _nodes} = chash

    cond do
      num === num_partitions ->
        ordered

      true ->
        {res, _} = Enum.split(num, ordered)
        res
    end
  end

  @spec update(index_as_int, chash_node, chash) :: chash
  def update(index_as_int, name, chash) do
    {num_partitions, nodes} = chash
    new_nodes = List.keyreplace(nodes, index_as_int, 1, {index_as_int, name})
    {num_partitions, new_nodes}
  end

  defp max_n(n, {num_partitions, _nodes}) do
    min(n, num_partitions)
  end

  @spec random_node(chash_node, chash_node) :: chash_node
  defp random_node(node_a, node_a), do: node_a
  defp random_node(node_a, node_b), do: Enum.random([node_a, node_b])
end
