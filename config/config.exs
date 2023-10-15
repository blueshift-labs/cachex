import Config

config :eredis_cluster,
  clusters: [
    [
      name: :dev,
      init_nodes: [
        {String.to_charlist("localhost"), 30001}
      ]
    ],
    [
      name: :test,
      init_nodes: [
        {String.to_charlist("localhost"), 30001}
      ]
    ]
  ],
  pool_size: 17,
  pool_max_overflow: 11
