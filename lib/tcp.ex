defmodule Sonix.Tcp do
  @moduledoc """
    TCP Connection Layer for Sonix
  """

  require Logger

  use Connection

  def start_link(host, port, args, gen_server_opts, timeout \\ 5000) do
    Connection.start_link(__MODULE__, {host, port, args, timeout}, gen_server_opts)
  end

  @doc """
  Send any Command to Sonic

  ## Examples

      iex> Sonix.send(conn, "PING")
      :ok
  """

  def send(conn, data), do: Connection.call(conn, {:send, data <> "\n"})

  @doc """
  Recieve response from Sonic

  ## Examples

      iex> Sonix.recv(conn)
      PONG
  """
  def recv(conn, bytes \\ 0, timeout \\ 3000) do
    with({:ok, response} <- Connection.call(conn, {:recv, bytes, timeout})) do
      case String.trim(response) do
        "ERR " <> reason -> {:error, reason}
        response -> {:ok, response}
      end
    else
      error -> error
    end
  end

  def close(conn), do: Connection.call(conn, :close)

  def init({host, port, opts, timeout}) do
    s = %{host: host, port: port, opts: opts, buffer: 20_000, timeout: timeout, sock: nil}
    {:connect, :init, s}
  end

  def connect(
        _,
        %{sock: nil, host: host, port: port, opts: opts, timeout: timeout} = s
      ) do
    case :gen_tcp.connect(host, port, [active: false] ++ opts, timeout) do
      {:ok, sock} ->
        {:ok, %{s | sock: sock}}

      {:error, _} ->
        {:backoff, 1000, s}
    end
  end

  def disconnect(info, %{sock: sock} = s) do
    :ok = :gen_tcp.close(sock)

    case info do
      {:close, from} ->
        Connection.reply(from, :ok)
        {:stop, :normal, s}

      {:error, :closed} ->
        Logger.error(fn -> "Connection closed" end)
        {:connect, :reconnect, %{s | sock: nil}}

      {:error, reason} ->
        Logger.error(fn -> "Connection error: #{inspect(reason)}" end)
        {:connect, :reconnect, %{s | sock: nil}}
    end
  end

  def handle_call(_, _, %{sock: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:send, data}, _, %{sock: sock, buffer: buffer} = s) do
    chunks = get_chunks(data, buffer)

    Enum.reduce_while(chunks, nil, fn ch, _acc ->
      Logger.info("Writing chunk: #{byte_size(ch)}")

      case :gen_tcp.send(sock, ch) do
        :ok ->
          {:cont, {:reply, :ok, s}}

        {:error, _} = error ->
          {:halt, {:disconnect, error, error, s}}
      end
    end)
  end

  def handle_call({:recv, bytes, timeout}, _, %{sock: sock} = s) do
    case do_recv(sock, bytes, timeout) do
      {:ok, res} ->
        {res, s} = handle_result({res, s})
        {:reply, {:ok, res}, s}

      {:error, :timeout} = error ->
        {:reply, error, s}

      {:error, _} = error ->
        {:disconnect, error, error, s}
    end
  end

  def handle_call(:close, from, s) do
    {:disconnect, {:close, from}, s}
  end

  defp get_chunks(data, buffer, chunks \\ []) do
    case byte_size(data) > buffer do
      true ->
        get_chunks(String.slice(data, 0..-buffer), buffer, [
          String.slice(data, -buffer, buffer) | chunks
        ])

      false ->
        [data | chunks]
    end
  end

  defp do_recv(responses \\ [], sock, bytes, timeout) do
    with({:ok, response} <- :gen_tcp.recv(sock, bytes, timeout)) do
      if String.ends_with?(response, "\r\n") do
        {:ok,
         responses
         |> Enum.reduce(response, &(&1 <> &2))
         |> String.trim()}
      else
        do_recv([response | responses], sock, bytes, timeout)
      end
    else
      error -> error
    end
  end

  def handle_result({<<"STARTED ", info::binary()>> = res, s}) do
    {res, %{s | buffer: get_buffer(info)}}
  end

  def handle_result({res, s}), do: {res, s}

  def get_buffer(info) do
    [_, _, buffer] = String.split(info)

    buffer
    |> String.slice(7..-2)
    |> String.to_integer()
  end
end
