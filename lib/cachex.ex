defmodule Cachex do
  @callback fetch(key :: term(), opts :: keyword()) :: {:ok, term()} | {:error, term()}

  def compile_config(opts) do
    {cache, opts} = Keyword.pop!(opts, :cache)
    {timeout, opts} = Keyword.pop(opts, :timeout, 5_000)
    {expiry, opts} = Keyword.pop(opts, :expiry, 60_000)

    {cache, timeout, expiry, opts}
  end

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer

      @behaviour Cachex

      {cache, timeout, expiry, opts} = Cachex.compile_config(opts)

      @cache cache
      @timeout timeout
      @expiry expiry

      def start_link(opts) do
        GenServer.start_link(__MODULE__, opts, name: __MODULE__)
      end

      def get(key, opts) do
        {timeout, opts} = Keyword.pop(opts, :timeout, @timeout)
        {expiry, opts} = Keyword.pop(opts, :expiry, @expiry)

        now = System.monotonic_time(:millisecond)

        case :ets.lookup(@cache, key) do
          [{^key, {value, ts}}] when now <= ts + expiry ->
            {:ok, value}

          [{^key, {value, _ts}}] ->
            GenServer.cast(__MODULE__, {:fetch, key, opts})
            {:ok, value}

          [] ->
            GenServer.call(__MODULE__, {:fetch, key, opts, expiry}, timeout)
        end
      end

      @impl true
      def init(opts) do
        Process.flag(:trap_exit, true)

        :ets.new(@cache, [
          :named_table,
          :set,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])

        {:ok, sup} = Task.Supervisor.start_link()

        {:ok, %{sup: sup, opts: opts, refs: %{}}}
      end

      @impl true
      def handle_cast({:fetch, key, opts}, %{sup: sup, opts: options, refs: refs} = state) do
        case Map.get(refs, key) do
          nil ->
            opts = Keyword.merge(options, opts)

            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, opts) end)

            refs = Map.put(refs, key, {task.ref, []})
            {:noreply, %{state | refs: refs}}

          {ref, _froms} ->
            {:noreply, state}
        end
      end

      @impl true
      def handle_call(
            {:fetch, key, opts, expiry},
            from,
            %{sup: sup, opts: options, refs: refs} = state
          ) do
        now = System.monotonic_time(:millisecond)

        case {:ets.lookup(@cache, key), Map.get(refs, key)} do
          {[{^key, {value, ts}}], _ref} when now <= ts + expiry ->
            {:reply, {:ok, value}, state}

          {[{^key, {value, _ts}}], nil} ->
            opts = Keyword.merge(options, opts)

            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, opts) end)

            refs = Map.put(refs, key, {task.ref, []})
            {:reply, {:ok, value}, %{state | refs: refs}}

          {[{^key, {value, _ts}}], _ref} ->
            {:reply, {:ok, value}, state}

          {[], nil} ->
            opts = Keyword.merge(options, opts)

            task = Task.Supervisor.async_nolink(sup, fn -> fetch(key, opts) end)

            refs = Map.put(refs, key, {task.ref, [from]})
            {:noreply, %{state | refs: refs}}

          {[], {ref, froms}} ->
            refs = Map.put(refs, key, {ref, [from | froms]})
            {:noreply, %{state | refs: refs}}
        end
      end

      @impl true
      def handle_info({ref, {:ok, value}}, %{refs: refs} = state) do
        Process.demonitor(ref, [:flush])

        {[{key, {_ref, froms}}], refs} =
          Enum.split_with(refs, &match?({_key, {^ref, _froms}}, &1))

        :ets.insert(@cache, {key, {value, System.monotonic_time(:millisecond)}})

        Enum.each(froms, &GenServer.reply(&1, {:ok, value}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      def handle_info({ref, {:error, err}}, %{refs: refs} = state) do
        Process.demonitor(ref, [:flush])

        {[{_key, {_ref, froms}}], refs} =
          Enum.split_with(refs, &match?({_key, {^ref, _froms}}, &1))

        Enum.each(froms, &GenServer.reply(&1, {:error, err}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      def handle_info({:DOWN, ref, :process, _pid, reason}, %{refs: refs} = state) do
        {[{_key, {_ref, froms}}], refs} =
          Enum.split_with(refs, &match?({_key, {^ref, _froms}}, &1))

        Enum.each(froms, &GenServer.reply(&1, {:error, reason}))

        {:noreply, %{state | refs: Enum.into(refs, %{})}}
      end

      @impl true
      def terminate(_reason, state) do
        IO.inspect("hellow")
        :ets.delete(@cache)

        state
      end

      @impl Cachex
      def fetch(_key, _opts) do
        {:error, :not_implemented}
      end

      defoverridable fetch: 2
    end
  end
end
