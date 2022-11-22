defmodule HTest do
  use ExUnit.Case

  setup do
    {:ok, conn: H.conn()}
  end

  # http pipelining doesn't seem to be implemented in ch
  # https://github.com/ClickHouse/ClickHouse/pull/4213 mentions "Hopefully poco doesn’t support pipelining, this may break things *."
  # poco is the http web server used by ch, no mentnion of http pipelining in its docs / github repo
  # https://github.com/pocoproject/poco/issues/2601 mentions "Caveat, breaks http pipelining, but who cares :)"
  # so I guess there is no pipelining support in ch if some features openly "break" it
  test "pipelining", %{conn: conn} do
    {:ok, conn, _ref1, _} = H.req(conn, "SELECT 1 + 1 FORMAT CSVWithNames")
    {:ok, conn, _ref2, _} = H.req(conn, "SELECT 100 + 222")
    socket = Mint.HTTP.get_socket(conn)

    # assert {:ok, conn, [_status, _headers | responses1]} = receive_stream(conn)
    # assert {:ok, conn, [_status, _headers | responses2]} = receive_stream(conn)
    # assert merge_body(responses1, ref1) == "2\n"
    # assert merge_body(responses2, ref2) == "3\n"

    assert [{:tcp, ^socket, response}] = flush()

    assert {:ok, {:http_response, {1, 1}, 200, "OK"}, rest} =
             :erlang.decode_packet(:http_bin, response, [])

    assert {:ok, headers, rest} = decode_headers(rest)

    assert [
             {"X-ClickHouse-Summary", _summary},
             {"Keep-Alive", "timeout=10"},
             {"X-ClickHouse-Timezone", "UTC"},
             {"X-ClickHouse-Format", "CSVWithNames"},
             {"X-ClickHouse-Query-Id", _query_id},
             {"Transfer-Encoding", "chunked"},
             {"X-ClickHouse-Server-Display-Name", _name},
             {"Content-Type", "text/csv; charset=UTF-8; header=present"},
             {"Connection", "Keep-Alive"},
             {"Date", _date}
           ] = headers

    assert NimbleCSV.RFC4180.parse_string(rest, skip_headers: false) == [
             ["D"],
             ["plus(1, 1)"],
             [""],
             ["2"],
             ["2"],
             [""]
           ]
  end

  def decode_headers(response, acc \\ []) do
    case :erlang.decode_packet(:httph_bin, response, []) do
      {:ok, {:http_header, _, _, k, v}, rest} -> decode_headers(rest, [{k, v} | acc])
      {:ok, :http_eoh, rest} -> {:ok, acc, rest}
      {:error, _reason} = error -> error
    end
  end

  def flush do
    receive do
      message -> [message | flush()]
    after
      5000 -> []
    end
  end

  # note that cancelling might not always work: https://github.com/ClickHouse/ClickHouse/issues/34397
  describe "cancel query" do
    setup %{conn: conn} do
      {:ok, conn, ref, query_id} = H.req(conn, "SELECT count() FROM system.numbers")
      {:ok, conn: conn, ref: ref, query_id: query_id}
    end

    test "from another conn", %{query_id: query_id} do
      conn = H.conn()

      {:ok, conn, ref, _query_id} = H.req(conn, "KILL QUERY WHERE query_id = '#{query_id}'")
      assert {:ok, _conn, [_status, _headers | responses]} = receive_stream(conn)
      body = merge_body(responses, ref)

      assert [["waiting", ^query_id, "default", "SELECT count() FROM system.numbers\\n"]] =
               TSV.parse_string(body, skip_headers: false)
    end

    # since http pipelining is not supported in clickhouse (see above)
    # this test cannot pass as the "kill query" (pipelined) request is ignored
    # TODO is there really no way?
    test "from current conn", %{conn: conn, query_id: query_id} do
      {:ok, conn, _ref, _query_id} = H.req(conn, "KILL QUERY WHERE query_id = '#{query_id}'")

      on_exit(fn ->
        {:ok, conn, _ref, _query_id} =
          H.req(H.conn(), "KILL QUERY WHERE query_id = '#{query_id}'")

        assert {:ok, _, _} = receive_stream(conn)
      end)

      assert {:ok, _conn, _resp} = receive_stream(conn)
    end
  end

  describe "cancel_http_readonly_queries_on_client_close=1" do
    test "when set to 1, query is killed when client disconnects", %{conn: conn} do
      qs =
        URI.encode_query(%{
          "query" => "SELECT count() FROM system.numbers",
          # it can also be set somewhere else in config.xml and it defaults to 0
          # https://clickhouse.com/docs/en/operations/settings/settings/#cancel-http-readonly-queries-on-client-close
          "cancel_http_readonly_queries_on_client_close" => "1"
        })

      path = "/?" <> qs
      query_id = Base.encode64(:crypto.strong_rand_bytes(16), padding: false)
      headers = [{"X-ClickHouse-Query-Id", query_id}]

      assert {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", path, headers, _body = "")
      assert {:ok, _conn} = Mint.HTTP.close(conn)

      :timer.sleep(100)

      {:ok, conn, ref, query_id2} =
        H.req(H.conn(), "SELECT query_id, query FROM system.processes")

      assert {:ok, _conn, [_status, _headers | responses]} = receive_stream(conn)
      body = merge_body(responses, ref)

      assert [[^query_id2, "SELECT query_id, query FROM system.processes\\n"]] =
               TSV.parse_string(body, skip_headers: false)
    end

    test "when set to 0, query continues after client disconnects", %{conn: conn} do
      qs =
        URI.encode_query(%{
          "query" => "SELECT count() FROM system.numbers",
          "cancel_http_readonly_queries_on_client_close" => "0"
        })

      path = "/?" <> qs
      query_id = Base.encode64(:crypto.strong_rand_bytes(16), padding: false)
      headers = [{"X-ClickHouse-Query-Id", query_id}]

      assert {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", path, headers, _body = "")
      assert {:ok, _conn} = Mint.HTTP.close(conn)

      on_exit(fn ->
        {:ok, conn, _ref, _query_id} =
          H.req(H.conn(), "KILL QUERY WHERE query_id = '#{query_id}'")

        assert {:ok, _, _} = receive_stream(conn)
      end)

      :timer.sleep(100)

      {:ok, conn, ref, query_id2} =
        H.req(H.conn(), "SELECT query_id, query FROM system.processes")

      assert {:ok, _conn, [_status, _headers | responses]} = receive_stream(conn)
      body = merge_body(responses, ref)

      assert [
               # still running
               [^query_id, "SELECT count() FROM system.numbers\\n"],
               [^query_id2, "SELECT query_id, query FROM system.processes\\n"]
             ] = TSV.parse_string(body, skip_headers: false)
    end
  end

  # we can simulate cancelling by ignoring the response to the cancelled request
  # but it seems to be no better
  test "ignore response", %{conn: conn} do
    {:ok, conn, ref, _query_id} = H.req(conn, "select 1 + 1")
    # imagine caller exits now
    ignored = [ref]

    # now image another request comes in, but we still have ignored responses on the socket
    # we send the new request and start waiting for it while dropping the ignored responses
    :timer.sleep(500)
    {:ok, conn, ref, _query_id} = H.req(conn, "select 1 + 2")

    assert {:ok, conn, [_status, _headers | responses]} = receive_stream(conn)
    _ignored = merge_body(responses, hd(ignored))

    assert {:ok, _conn, [_status, _headers | responses]} = receive_stream(conn)
    assert merge_body(responses, ref) == "3\n"
  end

  # def ignore_stream(conn, ignored, acc \\ []) do
  #   receive do
  #     message ->
  #       {:ok, conn, responses} = Mint.HTTP.stream(conn, message)
  #       {responses, ignored} = ignore_responses(responses, ignored, _acc = [])
  #       {:ok, conn, responses, ignored}
  #   after
  #     10000 ->
  #       flunk("ignore_stream timeout")
  #   end
  # end

  # def try_ignore_responses(responses, nothing_to_ignore = [], acc) do
  #   {:lists.reverse(acc) ++ responses, nothing_to_ignore}
  # end

  # def ignore_responses([{_, ref, _} | rest], [ref | _] = ignored, acc) do
  #   ignore_responses(rest, ignored, acc)
  # end

  # def ignore_responses([{:done, ref} | rest], [ref | other_ignored], acc) do
  #   ignore_responses(rest, other_ignored, acc)
  # end

  # def ignore_responses([response | rest], ignored, acc) do
  #   ignore_responses(rest, ignored, [response | acc])
  # end

  # def ignore_responses([], ignored, acc) do
  #   {:lists.reverse(acc), ignored}
  # end

  def receive_stream(conn) do
    receive do
      {:rest, previous} -> maybe_done(conn, previous)
    after
      0 -> receive_stream(conn, [])
    end
  end

  def receive_stream(conn, acc) do
    socket = Mint.HTTP.get_socket(conn)

    receive do
      {tag, ^socket, _data} = message when tag in [:tcp, :ssl] ->
        assert {:ok, conn, responses} = conn.__struct__.stream(conn, message)
        maybe_done(conn, acc ++ responses)

      {tag, ^socket} = message when tag in [:tcp_closed, :ssl_closed] ->
        assert {:ok, conn, responses} = conn.__struct__.stream(conn, message)
        maybe_done(conn, acc ++ responses)

      {tag, ^socket, _reason} = message when tag in [:tcp_error, :ssl_error] ->
        assert {:error, _conn, _reason, _responses} = conn.__struct__.stream(conn, message)
    after
      10000 ->
        flunk("receive_stream timeout")
    end
  end

  def maybe_done(conn, responses) do
    {all, rest} = Enum.split_while(responses, &(not match?({:done, _}, &1)))

    case {all, rest} do
      {all, []} ->
        receive_stream(conn, all)

      {all, [done | rest]} ->
        if rest != [], do: send(self(), {:rest, rest})
        {:ok, conn, all ++ [done]}
    end
  end

  def merge_body(responses, request) do
    merge_body(responses, request, "")
  end

  defp merge_body([{:data, request, new_body} | responses], request, body) do
    merge_body(responses, request, body <> new_body)
  end

  defp merge_body([{:done, request}], request, body) do
    body
  end
end
