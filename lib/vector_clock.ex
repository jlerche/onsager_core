defmodule OnsagerCore.VectorClock do
  @moduledoc """
  Vector clock implementation
  """

  @type vclock_node :: term
  @type counter :: integer
  @type timestamp :: integer
  @type dot :: {vclock_node, {counter, timestamp}}
  @type pure_dot :: {vclock_node, counter}
  @type vclock :: [dot]

  @days_from_gregorian_base_to_epoch 1970 * 365 + 478
  @seconds_from_gregorian_base_to_epoch @days_from_gregorian_base_to_epoch * 24 * 60 * 60

  @spec fresh :: vclock
  def fresh, do: []

  @spec fresh(vclock_node, counter) :: vclock
  def fresh(node, count), do: [{node, {count, timestamp()}}]

  @spec descends(vclock, vclock) :: boolean
  def descends(_, []), do: true

  def descends(vec_a, vec_b) do
    [{node_b, {counter_b, _timestamp}} | tail_b] = vec_b

    case List.keyfind(vec_a, node_b, 1) do
      nil ->
        false

      {_, {counter_a, _timestamp_a}} ->
        # rewrite to case statements?
        counter_a >= counter_b and descends(vec_a, tail_b)
    end
  end

  @spec descends_dot(vclock, dot) :: boolean
  def descends_dot(v_clock, dot), do: descends(v_clock, [dot])

  @spec pure_dot(dot) :: pure_dot
  def pure_dot({node, {counter, _timestamp}}), do: {node, counter}

  @spec dominates(vclock, vclock) :: boolean
  def dominates(vec_a, vec_b), do: descends(vec_a, vec_b) and not descends(vec_b, vec_a)

  @spec merge([vclock]) :: vclock
  def merge(vclocks)
  def merge([]), do: []

  def merge([single_vec_clock]), do: [single_vec_clock]

  def merge([head | tail]), do: merge(tail, List.keysort(head, 1))

  def merge([], n_clock), do: n_clock

  def merge([head | tail], n_clock), do: merge(tail, merge(List.keysort(head, 1), n_clock, []))

  def merge([], [], acc_clock), do: Enum.reverse(acc_clock)

  def merge([], left, acc_clock), do: Enum.reverse(acc_clock, left)

  def merge(left, [], acc_clock), do: Enum.reverse(acc_clock, left)

  def merge(
        v_vec = [{node1, {ctr1, ts1} = ct1} = nct1 | v_clock],
        n_vec = [{node2, {ctr2, ts2} = ct2} = nct2 | n_clock],
        acc_clock
      ) do
    cond do
      node1 < node2 ->
        merge(v_clock, n_vec, [nct1 | acc_clock])

      node1 > node2 ->
        merge(v_vec, n_clock, [nct2 | acc_clock])

      true ->
        ({_ctr, _ts} = ct) =
          cond do
            ctr1 > ctr2 -> ct1
            ctr1 < ctr2 -> ct2
            true -> {ctr1, max(ts1, ts2)}
          end

        merge(v_clock, n_clock, [{node1, ct} | acc_clock])
    end
  end

  @spec get_counter(vclock_node, vclock) :: counter
  def get_counter(node, vec_clock) do
    case List.keyfind(vec_clock, node, 1) do
      {_, {ctr, _ts}} -> ctr
      nil -> 0
    end
  end

  @spec get_timestamp(vclock_node, vclock) :: timestamp | :undefined
  def get_timestamp(node, vec_clock) do
    case List.keyfind(vec_clock, node, 1) do
      {_, {_ctr, ts}} -> ts
      nil -> :undefined
    end
  end

  @spec get_dot(vclock_node, vclock) :: {:ok, dot} | :undefined
  def get_dot(node, vec_clock) do
    case List.keyfind(vec_clock, node, 1) do
      nil -> :undefined
      entry -> {:ok, entry}
    end
  end

  @spec valid_dot(dot) :: boolean
  def valid_dot({_, {count, ts}}) when is_integer(count) when is_integer(ts) do
    true
  end

  def valid_dot(_), do: false

  @spec increment(vclock_node, vclock) :: vclock
  def increment(node, vec_clock), do: increment(node, timestamp(), vec_clock)

  @spec increment(vclock_node, timestamp, vclock) :: vclock
  def increment(node, inc_ts, vec_clock) do
    {{_ctr, _ts} = c1, new_vec} =
      case List.keytake(vec_clock, node, 1) do
        false -> {{1, inc_ts}, vec_clock}
        {:value, {_nde, {cnt, _time_stamp}}, mod_vec} -> {{cnt + 1, inc_ts}, mod_vec}
      end

    [{node, c1} | new_vec]
  end

  @spec all_nodes(vclock) :: [vclock_node]
  def all_nodes(vec_clock), do: for({x, {_, _}} <- vec_clock, do: x)

  @spec timestamp() :: timestamp
  def timestamp() do
    {mega_seconds, seconds, _} = :os.timestamp()
    @seconds_from_gregorian_base_to_epoch + mega_seconds * 1_000_000 + seconds
  end

  @spec equal(vclock, vclock) :: boolean
  def equal(vec_a, vec_b), do: Enum.sort(vec_a) === Enum.sort(vec_b)

  @spec prune(vclock, integer, term) :: vclock
  def prune(vec, time_now, bucket_props) do
    sort_vec =
      Enum.sort(vec, fn {node1, {_, ts1}}, {node2, {_, ts2}} -> {ts1, node1} < {ts2, node2} end)

    prune_vclock(sort_vec, time_now, bucket_props)
  end

  defp prune_vclock(vec, time_now, bucket_props) do
    case length(vec) <= get_property(:small_vclock, bucket_props) do
      true ->
        vec

      false ->
        {_, {_, head_time}} = hd(vec)

        case time_now - head_time < get_property(:young_vclock, bucket_props) do
          true -> vec
          false -> prune_vclock(vec, time_now, bucket_props, head_time)
        end
    end
  end

  defp prune_vclock(vec, time_now, bucket_props, head_time) do
    case length(vec) > get_property(:big_vclock, bucket_props) or
           time_now - head_time > get_property(:old_vclock, bucket_props) do
      true -> prune_vclock(tl(vec), time_now, bucket_props)
      false -> vec
    end
  end

  defp get_property(key, pair_list) do
    case List.keyfind(pair_list, key, 1) do
      {_key, value} -> value
      nil -> :undefined
    end
  end
end
