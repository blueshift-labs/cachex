defmodule Cachex do
  @callback fetch(key :: atom(), params :: map(), opts :: keyword()) ::
              {:ok, term()} | {:error, term()}

  def compile_config(opts) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 5_000)
    {expiry, _opts} = Keyword.pop(opts, :expiry, 60_000)

    {timeout, expiry}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer

      @behaviour Cachex

      {timeout, expiry} = Cachex.compile_config(opts)

      @timeout timeout
      @expiry expiry

      def start_link([]) do
        GenServer.start_link(__MODULE__, [], name: __MODULE__)
      end

      def get(key, params, opts) do
        {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
        {expiry, opts} = Keyword.pop(opts, :expiry, @expiry)

        now = System.monotonic_time(:millisecond)

        case :ets.lookup(__MODULE__, {key, params}) do
          [{_, {value, ts}}] when now <= ts + expiry ->
            {:ok, value}

          [{_, {value, _ts}}] ->
            GenServer.cast(__MODULE__, {:fetch, key, params, opts})
            {:ok, value}

          [] ->
            GenServer.call(__MODULE__, {:fetch, key, params, opts, expiry}, timeout)
        end
      end

      @impl true
      def init([]) do
        Process.flag(:trap_exit, true)

        :ets.new(__MODULE__, [
          :named_table,
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])

        {:ok, sup} = Task.Supervisor.start_link()

        {:ok, %{sup: sup, refs: %{}}}
      end

      @impl true
      def handle_cast({:fetch, key, params, opts}, %{sup: sup, refs: refs} = state) do
        case Map.get(refs, {key, params}) do
          nil ->
            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, params, opts) end)

            refs = Map.put(refs, {key, params}, {task.ref, [], System.monotonic_time()})
            {:noreply, %{state | refs: refs}}

          {ref, _froms, _ts} ->
            {:noreply, state}
        end
      end

      @impl true
      def handle_call(
            {:fetch, key, params, opts, expiry},
            from,
            %{sup: sup, refs: refs} = state
          ) do
        now = System.monotonic_time(:millisecond)

        case {:ets.lookup(__MODULE__, {key, params}), Map.get(refs, {key, params})} do
          {[{_, {value, ts}}], _ref} when now <= ts + expiry ->
            {:reply, {:ok, value}, state}

          {[{_, {value, _ts}}], nil} ->
            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, params, opts) end)

            refs = Map.put(refs, {key, params}, {task.ref, [], System.monotonic_time()})
            {:reply, {:ok, value}, %{state | refs: refs}}

          {[{_, {value, _ts}}], _ref} ->
            {:reply, {:ok, value}, state}

          {[], nil} ->
            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, params, opts) end)

            refs = Map.put(refs, {key, params}, {task.ref, [from], System.monotonic_time()})
            {:noreply, %{state | refs: refs}}

          {[], {ref, froms, ts}} ->
            refs = Map.put(refs, {key, params}, {ref, [from | froms], ts})
            {:noreply, %{state | refs: refs}}
        end
      end

      @impl true
      def handle_info({ref, {:ok, value}}, %{refs: refs} = state) do
        Process.demonitor(ref, [:flush])

        {[{{key, params}, {_ref, froms, ts}}], refs} =
          Enum.split_with(refs, &match?({{_key, _params}, {^ref, _froms, _ts}}, &1))

        :telemetry.execute(
          [:cachex, :fetch, :success],
          %{count: 1, duration: System.monotonic_time() - ts},
          %{cache: __MODULE__, key: key, params: params}
        )

        :ets.insert(__MODULE__, {{key, params}, {value, System.monotonic_time(:millisecond)}})

        Enum.each(froms, &GenServer.reply(&1, {:ok, value}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      def handle_info({ref, {:error, err}}, %{refs: refs} = state) do
        Process.demonitor(ref, [:flush])

        {[{{key, params}, {_ref, froms, ts}}], refs} =
          Enum.split_with(refs, &match?({{_key, _params}, {^ref, _froms, _ts}}, &1))

        :telemetry.execute(
          [:cachex, :fetch, :error],
          %{count: 1, duration: System.monotonic_time() - ts},
          %{cache: __MODULE__, key: key, params: params}
        )

        Enum.each(froms, &GenServer.reply(&1, {:error, err}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      def handle_info({:DOWN, ref, :process, _pid, reason}, %{refs: refs} = state) do
        {[{{key, params}, {_ref, froms, ts}}], refs} =
          Enum.split_with(refs, &match?({{_key, _params}, {^ref, _froms, _ts}}, &1))

        :telemetry.execute(
          [:cachex, :fetch, :error],
          %{count: 1, duration: System.monotonic_time() - ts},
          %{cache: __MODULE__, key: key, params: params}
        )

        Enum.each(froms, &GenServer.reply(&1, {:error, reason}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      @impl true
      def terminate(_reason, state) do
        :ets.delete(__MODULE__)

        state
      end

      @impl Cachex
      def fetch(_key, _params, _opts) do
        {:error, :not_implemented}
      end

      defoverridable fetch: 3
    end
  end
end
