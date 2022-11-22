NimbleCSV.define(TSV,
  separator: "\t",
  escape: "\"",
  line_separator: "\r\n",
  moduledoc: """
  A TSV parser that uses tab as separator and double-quotes as escape.
  """
)

defmodule H do
  @moduledoc """
  Documentation for `H`.
  """

  require Logger

  def conn(opts \\ []) do
    {:ok, conn} = Mint.HTTP.connect(:http, "localhost", 8123, opts)
    conn
  end

  def pipeline(conn \\ conn(), query1, query2) do
    qs1 = URI.encode_query(%{"query" => query1})
    qs2 = URI.encode_query(%{"query" => query2})
    path1 = "/?" <> qs1
    path2 = "/?" <> qs2
    headers = []
    body = ""
    {:ok, conn, ref1} = Mint.HTTP.request(conn, "GET", path1, headers, body)
    {:ok, conn, ref2} = Mint.HTTP.request(conn, "GET", path2, headers, body)

    {:ok, conn, ref1, ref2}

    # {{:ok, messages, resp1}, conn} =
    #   recv_loop(conn, ref1, _response = {_status = nil, _headers = nil, _data = []}, _prev = [])

    # {{:ok, messages, resp2}, conn} =
    #   recv_loop(conn, ref2, _response = {_status = nil, _headers = nil, _data = []}, messages)

    # {messages, conn, resp1, resp2}
  end

  def long_quiery() do
  end

  def req(conn, query, body \\ "") do
    qs = URI.encode_query(%{"query" => query})
    path = "/?" <> qs
    query_id = Base.encode64(:crypto.strong_rand_bytes(16), padding: false)
    headers = [{"X-ClickHouse-Query-Id", query_id}]

    with {:ok, conn, ref} <- Mint.HTTP.request(conn, "GET", path, headers, body) do
      {:ok, conn, ref, query_id}
    end

    # recv_loop(conn, ref, _response = {_status = nil, _headers = nil, _data = []})
  end

  def recv_loop(conn, ref, response) do
    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, messages} ->
            # TODO filter by ref?
            case handle_response(messages, response) do
              {:ok, _messages, _response} = ok -> {ok, conn}
              {:more, response} -> recv_loop(conn, ref, response)
            end

          :unknown ->
            Logger.warning("unknown message: #{inspect(message)}")
            recv_loop(conn, ref, response)
        end
    after
      5000 ->
        Logger.warning("5s passed, still waiting...")
        recv_loop(conn, ref, response)
    end
  end

  def handle_response([{:data, _ref, data} | rest], {_, _, prev_data} = resp) do
    handle_response(rest, put_elem(resp, 2, [prev_data | data]))
  end

  def handle_response([{:status, _ref, status} | rest], resp) do
    handle_response(rest, put_elem(resp, 0, status))
  end

  def handle_response([{:headers, _ref, headers} | rest], resp) do
    handle_response(rest, put_elem(resp, 1, headers))
  end

  def handle_response([], resp) do
    {:more, resp}
  end

  def handle_response([{:done, _ref} | rest], resp) do
    {:ok, rest, resp}
  end
end
