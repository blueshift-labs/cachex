defmodule Cachex.Redis do
  @callback encode_key(key :: atom(), params :: map()) :: {:ok, binary()} | {:error, term()}

  @callback decode_key(key :: binary()) :: {:ok, {atom(), map()}} | {:error, term()}

  @callback encode_value(term()) :: {:ok, binary()} | {:error, term()}

  @callback decode_value(binary()) :: {:ok, term()} | {:error, term()}

  def compile_config(opts) do
    {cluster, opts} = Keyword.pop(opts, :cluster, :eredis_cluster_default)
    {timeout, opts} = Keyword.pop(opts, :timeout, 5_000)
    {expiry, _opts} = Keyword.pop(opts, :expiry, 60_000)

    {cluster, timeout, expiry}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer

      @behaviour Cachex

      @behaviour Cachex.Redis

      {cluster, timeout, expiry} = Cachex.Redis.compile_config(opts)

      @cluster cluster
      @timeout timeout
      @expiry expiry

      def start_link([]) do
        GenServer.start_link(__MODULE__, [], name: __MODULE__)
      end

      def get(key, params, opts) do
        if Keyword.get(opts, :force?, false) do
          force_get(key, params, opts)
        else
          {timeout, opts} = Keyword.get(opts, :timeout, @timeout)
          {expiry, opts} = Keyword.get(opts, :expiry, @expiry)

          with {:ok, encoded_key} <- encode_key(key, params) do
            case :eredis_cluster.q(@cluster, ["GET", encoded_key]) do
              {:ok, :undefined} ->
                GenServer.call(__MODULE__, {:fetch, key, params, opts, expiry}, timeout)

              {:ok, encoded_value} ->
                decode_value(encoded_value)

              err ->
                err
            end
          end
        end
      end

      defp force_get(key, params, opts) do
        {expiry, opts} = Keyword.get(opts, :expiry, @expiry)

        with {:ok, value} <- fetch(key, params, opts),
             {:ok, encoded_key} <- encode_key(key, params),
             {:ok, encoded_value} <- encode_value(value),
             {:ok, "OK"} <-
               :eredis_cluster.q(@cluster, ["SET", encoded_key, encoded_value, "PX", expiry]) do
          {:ok, value}
        end
      end

      @impl true
      def init([]), do: {:ok, %{}}

      @impl true
      def handle_call({:fetch, key, params, opts, expiry}, _from, state) do
        with {:ok, encoded_key} <- encode_key(key, params),
             {:ok, :undefined} <- :eredis_cluster.q(@cluster, ["GET", encoded_key]),
             {:ok, value} <- fetch(key, params, opts),
             {:ok, encoded_value} <- encode_value(value),
             {:ok, "OK"} <-
               :eredis_cluster.q(@cluster, ["SET", encoded_key, encoded_value, "PX", expiry]) do
          {:reply, {:ok, value}, state}
        else
          {:ok, encoded_value} ->
            {:reply, decode_value(encoded_value), state}

          err ->
            {:reply, err, state}
        end
      end

      @impl Cachex
      def fetch(_key, _params, _opts) do
        {:error, :not_implemented}
      end

      defoverridable fetch: 3

      @impl Cachex.Redis
      def encode_key(key :: atom(), params :: map()) do
        {:error, :not_implemented}
      end

      @impl Cachex.Redis
      def decode_key(key :: binary()) do
        {:error, :not_implemented}
      end

      @impl Cachex.Redis
      def encode_value(term()) do
        {:error, :not_implemented}
      end

      @impl Cachex.Redis
      def decode_value(binary()) do
        {:error, :not_implemented}
      end

      defoverridable encode_key: 2, decode_key: 1, encode_value: 1, decode_value: 1
    end
  end
end
