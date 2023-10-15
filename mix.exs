defmodule Cachex.MixProject do
  use Mix.Project

  def project do
    [
      app: :cachex,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:eredis_cluster,
       git: "https://github.com/blueshift-labs/eredis_cluster.git",
       tag: "bsft-0.9.0",
       optional: true},
      {:telemetry, "~> 1.1"}
    ]
  end
end
