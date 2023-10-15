defmodule Cachex do
  @callback fetch(key :: atom(), params :: map(), opts :: keyword()) ::
              {:ok, term()} | {:error, term()}
end
