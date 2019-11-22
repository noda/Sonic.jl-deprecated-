# Sonic.jl
Julia package for analytics and control.

Example use in Julia REPL:

```
(v1.2) pkg> activate .
Activating environment at `~/Sonic.jl/Project.toml`

julia> import Sonic

julia> Sonic.Linckii.get_site_structure("keab")
Table with 4721 rows, 13 columns:
Columns:
#   colname             type
──────────────────────────────
1   sensor_id           Int64
2   device_id           Int64
3   node_enabled        Bool
4   node_name           String
5   node_public         Bool
6   node_description    String
7   node_owner          Bool
8   node_id             Int64
9   protocol_id         Int64
10  device_name         String
11  sensor_unit         Any
12  sensor_name         String
13  sensor_description  String
```
