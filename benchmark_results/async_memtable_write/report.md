# async_memtable_write

Status: 504/504 runs collected.

Runtime: lb_rocksdb_16u container, CPU quota 16, cpuset 0-15 (8 physical cores / 16 SMT threads).
Full methodology: [README.md](README.md).

Source: 97fc7d395; zero range reverted. Release -O2.
32 clients, 16B keys, 100B values, 1M keys. 10 seconds per run.
Three independent repetitions, randomized order; throughput is the median.
WAL enabled. SST seed. P99 is API latency; throughput includes async drain.
Speedup uses the same workload, sync, concurrent and pipeline native baseline.
P99 columns show the median P99 across runs, in microseconds.

## Best async configuration for each publication mode

|Workload|Sync|Lazy|Native ops/s|Async ops/s|Change|Workers / C / pipeline|Read P99|Write P99|
|---|---|---|---|---|---|---|---|---|
|readrandom|False|False|3153601|3159712|+0.2%|1/False/false|6.56|—|
|readrandom|False|True|3153601|3158234|+0.1%|8/True/false|6.36|—|
|readrandom|True|False|3155801|3157488|+0.1%|2/True/true|6.57|—|
|readrandom|True|True|3155801|3166862|+0.4%|1/True/false|6.45|—|
|readrandomwriterandom|False|False|913228|826661|-9.5%|1/True/false|20.61|467.50|
|readrandomwriterandom|False|True|913228|985693|+7.9%|2/True/false|206.10|358.68|
|readrandomwriterandom|True|False|437289|298161|-31.8%|2/True/false|16.78|368.62|
|readrandomwriterandom|True|True|437289|339615|-22.3%|1/False/true|168.92|217.28|
|overwrite|False|False|555173|542168|-2.3%|2/True/true|—|154.15|
|overwrite|False|True|555173|964314|+73.7%|2/True/true|—|70.00|
|overwrite|True|False|222516|161227|-27.5%|4/True/true|—|342.64|
|overwrite|True|True|222516|308135|+38.5%|16/True/true|—|166.70|

## Best configurations (across pipeline and concurrency settings)

|Workload|Sync|Best native ops/s|Best async ops/s|Change|Async workers / lazy / C / pipeline|
|---|---|---|---|---|---|
|readrandom|False|3153601|3159712|+0.2%|1 / False / False / false|
|readrandom|True|3155801|3166862|+0.4%|1 / True / True / false|
|readrandomwriterandom|False|913228|985693|+7.9%|2 / True / True / false|
|readrandomwriterandom|True|437289|339615|-22.3%|1 / True / False / true|
|overwrite|False|555173|964314|+73.7%|2 / True / True / true|
|overwrite|True|222516|308135|+38.5%|16 / True / True / true|

## readrandom
|pipeline|sync|concurrent|workers|lazy|ops/s|speedup|P99 read/write|CV|runs|
|---|---|---|---|---|---|---|---|---|---|
|false|False|False|0|None|3097540|1.000|6.53/—|1.4%|3|
|false|False|False|1|False|3159712|1.020|6.56/—|0.2%|3|
|false|False|False|1|True|3130942|1.011|6.60/—|0.4%|3|
|false|False|True|0|None|3094272|1.000|6.38/—|1.7%|3|
|false|False|True|1|False|3137598|1.014|6.59/—|0.7%|3|
|false|False|True|1|True|3155596|1.020|6.65/—|0.4%|3|
|false|False|True|2|False|3158279|1.021|6.42/—|0.3%|3|
|false|False|True|2|True|3146487|1.017|6.62/—|0.8%|3|
|false|False|True|4|False|3093912|1.000|6.49/—|0.2%|3|
|false|False|True|4|True|3107488|1.004|6.59/—|1.4%|3|
|false|False|True|8|False|3097752|1.001|6.47/—|1.6%|3|
|false|False|True|8|True|3158234|1.021|6.36/—|0.9%|3|
|false|False|True|16|False|3117853|1.008|6.82/—|1.5%|3|
|false|False|True|16|True|3139303|1.015|6.39/—|0.4%|3|
|false|True|False|0|None|3112740|1.000|6.64/—|0.8%|3|
|false|True|False|1|False|3092460|0.993|6.74/—|1.0%|3|
|false|True|False|1|True|3145521|1.011|6.74/—|1.0%|3|
|false|True|True|0|None|3155801|1.000|6.29/—|0.6%|3|
|false|True|True|1|False|3128818|0.991|6.70/—|0.3%|3|
|false|True|True|1|True|3166862|1.004|6.45/—|0.6%|3|
|false|True|True|2|False|3150631|0.998|6.52/—|1.3%|3|
|false|True|True|2|True|3125058|0.990|6.50/—|0.6%|3|
|false|True|True|4|False|3122909|0.990|6.36/—|0.7%|3|
|false|True|True|4|True|3146194|0.997|6.82/—|0.5%|3|
|false|True|True|8|False|3149837|0.998|6.76/—|1.5%|3|
|false|True|True|8|True|3149146|0.998|6.64/—|0.2%|3|
|false|True|True|16|False|3096101|0.981|6.56/—|0.4%|3|
|false|True|True|16|True|3142288|0.996|6.72/—|0.4%|3|
|true|False|False|0|None|3142748|1.000|6.40/—|0.5%|3|
|true|False|False|1|False|3093965|0.984|6.53/—|0.6%|3|
|true|False|False|1|True|3128356|0.995|6.65/—|1.0%|3|
|true|False|True|0|None|3153601|1.000|6.34/—|0.3%|3|
|true|False|True|1|False|3151384|0.999|6.62/—|0.3%|3|
|true|False|True|1|True|3143494|0.997|6.90/—|0.7%|3|
|true|False|True|2|False|3151499|0.999|6.68/—|0.7%|3|
|true|False|True|2|True|3125640|0.991|6.55/—|1.4%|3|
|true|False|True|4|False|3108849|0.986|6.45/—|1.4%|3|
|true|False|True|4|True|3152298|1.000|6.40/—|0.6%|3|
|true|False|True|8|False|3141146|0.996|6.53/—|1.0%|3|
|true|False|True|8|True|3148669|0.998|6.40/—|0.8%|3|
|true|False|True|16|False|3142700|0.997|6.29/—|0.4%|3|
|true|False|True|16|True|3134615|0.994|6.51/—|0.3%|3|
|true|True|False|0|None|3150249|1.000|6.75/—|0.7%|3|
|true|True|False|1|False|3148407|0.999|6.48/—|0.5%|3|
|true|True|False|1|True|3135360|0.995|6.23/—|0.1%|3|
|true|True|True|0|None|3112033|1.000|6.58/—|0.7%|3|
|true|True|True|1|False|3126283|1.005|6.50/—|0.7%|3|
|true|True|True|1|True|3112662|1.000|6.58/—|1.4%|3|
|true|True|True|2|False|3157488|1.015|6.57/—|1.1%|3|
|true|True|True|2|True|3135492|1.008|6.56/—|1.3%|3|
|true|True|True|4|False|3145078|1.011|6.47/—|0.8%|3|
|true|True|True|4|True|3104016|0.997|6.73/—|0.1%|3|
|true|True|True|8|False|3111971|1.000|6.59/—|0.6%|3|
|true|True|True|8|True|3142806|1.010|6.56/—|1.3%|3|
|true|True|True|16|False|3148939|1.012|6.62/—|0.8%|3|
|true|True|True|16|True|3140908|1.009|6.79/—|0.9%|3|

## readrandomwriterandom
|pipeline|sync|concurrent|workers|lazy|ops/s|speedup|P99 read/write|CV|runs|
|---|---|---|---|---|---|---|---|---|---|
|false|False|False|0|None|885557|1.000|19.89/109.46|0.5%|3|
|false|False|False|1|False|822124|0.928|20.57/471.88|0.3%|3|
|false|False|False|1|True|808045|0.912|780.82/177.27|0.5%|3|
|false|False|True|0|None|664427|1.000|19.32/499.77|0.1%|3|
|false|False|True|1|False|826661|1.244|20.61/467.50|0.2%|3|
|false|False|True|1|True|806192|1.213|784.50/182.67|0.2%|3|
|false|False|True|2|False|767808|1.156|20.51/482.49|0.3%|3|
|false|False|True|2|True|985693|1.484|206.10/358.68|0.1%|3|
|false|False|True|4|False|709083|1.067|20.39/480.45|0.8%|3|
|false|False|True|4|True|898927|1.353|156.78/387.23|0.4%|3|
|false|False|True|8|False|671287|1.010|20.32/482.49|0.7%|3|
|false|False|True|8|True|819587|1.234|164.66/393.01|0.5%|3|
|false|False|True|16|False|678875|1.022|20.39/468.71|0.5%|3|
|false|False|True|16|True|820378|1.235|166.20/382.12|0.3%|3|
|false|True|False|0|None|326396|1.000|14.81/249.68|5.1%|3|
|false|True|False|1|False|294860|0.903|14.87/370.16|1.0%|3|
|false|True|False|1|True|330685|1.013|169.08/218.04|1.4%|3|
|false|True|True|0|None|332270|1.000|14.85/269.23|7.7%|3|
|false|True|True|1|False|296615|0.893|14.88/369.53|2.0%|3|
|false|True|True|1|True|332503|1.001|169.25/216.93|3.0%|3|
|false|True|True|2|False|298161|0.897|16.78/368.62|0.1%|3|
|false|True|True|2|True|324065|0.975|169.15/217.59|4.0%|3|
|false|True|True|4|False|294316|0.886|16.39/372.51|0.8%|3|
|false|True|True|4|True|325595|0.980|168.97/221.19|0.8%|3|
|false|True|True|8|False|294870|0.887|14.96/369.83|1.7%|3|
|false|True|True|8|True|331309|0.997|169.11/217.42|1.9%|3|
|false|True|True|16|False|294934|0.888|14.99/370.53|0.7%|3|
|false|True|True|16|True|323312|0.973|169.25/219.50|6.4%|3|
|true|False|False|0|None|913228|1.000|20.37/133.88|0.1%|3|
|true|False|False|1|False|824118|0.902|20.58/466.63|0.4%|3|
|true|False|False|1|True|807843|0.885|779.44/177.92|0.2%|3|
|true|False|True|0|None|625464|1.000|19.57/511.57|0.4%|3|
|true|False|True|1|False|825635|1.320|20.56/468.09|0.4%|3|
|true|False|True|1|True|806550|1.290|775.24/173.92|0.3%|3|
|true|False|True|2|False|767001|1.226|20.56/482.08|0.8%|3|
|true|False|True|2|True|982351|1.571|205.32/361.57|0.4%|3|
|true|False|True|4|False|704567|1.126|20.39/486.35|0.8%|3|
|true|False|True|4|True|898714|1.437|158.96/382.70|0.5%|3|
|true|False|True|8|False|673024|1.076|20.33/479.29|0.7%|3|
|true|False|True|8|True|825608|1.320|165.00/388.91|0.2%|3|
|true|False|True|16|False|679907|1.087|20.50/465.05|0.5%|3|
|true|False|True|16|True|821912|1.314|165.22/393.46|0.6%|3|
|true|True|False|0|None|437289|1.000|17.93/219.65|8.3%|3|
|true|True|False|1|False|294674|0.674|14.87/370.73|1.1%|3|
|true|True|False|1|True|339615|0.777|168.92/217.28|5.0%|3|
|true|True|True|0|None|412349|1.000|18.92/285.13|1.8%|3|
|true|True|True|1|False|297605|0.722|14.91/369.13|0.6%|3|
|true|True|True|1|True|330317|0.801|169.00/216.71|7.6%|3|
|true|True|True|2|False|289378|0.702|15.22/370.11|3.9%|3|
|true|True|True|2|True|329642|0.799|168.95/219.80|0.7%|3|
|true|True|True|4|False|293640|0.712|17.10/372.25|1.1%|3|
|true|True|True|4|True|329162|0.798|168.92/219.40|1.6%|3|
|true|True|True|8|False|289839|0.703|15.62/371.36|5.0%|3|
|true|True|True|8|True|325083|0.788|168.96/221.41|1.5%|3|
|true|True|True|16|False|297072|0.720|15.88/371.43|3.0%|3|
|true|True|True|16|True|322349|0.782|169.04/220.75|3.0%|3|

## overwrite
|pipeline|sync|concurrent|workers|lazy|ops/s|speedup|P99 read/write|CV|runs|
|---|---|---|---|---|---|---|---|---|---|
|false|False|False|0|None|521788|1.000|—/108.11|0.4%|3|
|false|False|False|1|False|515587|0.988|—/119.03|0.9%|3|
|false|False|False|1|True|613690|1.176|—/69.33|0.6%|3|
|false|False|True|0|None|473425|1.000|—/109.97|1.5%|3|
|false|False|True|1|False|506229|1.069|—/116.25|1.0%|3|
|false|False|True|1|True|618209|1.306|—/68.45|0.7%|3|
|false|False|True|2|False|541016|1.143|—/154.01|0.2%|3|
|false|False|True|2|True|961065|2.030|—/70.09|0.2%|3|
|false|False|True|4|False|487212|1.029|—/163.12|0.5%|3|
|false|False|True|4|True|922776|1.949|—/74.65|0.3%|3|
|false|False|True|8|False|450288|0.951|—/166.55|0.2%|3|
|false|False|True|8|True|779329|1.646|—/99.51|0.3%|3|
|false|False|True|16|False|455252|0.962|—/165.95|0.6%|3|
|false|False|True|16|True|778564|1.645|—/101.59|0.1%|3|
|false|True|False|0|None|171215|1.000|—/249.59|7.1%|3|
|false|True|False|1|False|157752|0.921|—/354.65|0.2%|3|
|false|True|False|1|True|306607|1.791|—/166.79|1.4%|3|
|false|True|True|0|None|176514|1.000|—/249.76|5.6%|3|
|false|True|True|1|False|157811|0.894|—/354.32|9.0%|3|
|false|True|True|1|True|305336|1.730|—/166.39|1.6%|3|
|false|True|True|2|False|156863|0.889|—/352.69|5.9%|3|
|false|True|True|2|True|301061|1.706|—/167.06|0.7%|3|
|false|True|True|4|False|158455|0.898|—/352.91|1.8%|3|
|false|True|True|4|True|304691|1.726|—/166.52|1.0%|3|
|false|True|True|8|False|154901|0.878|—/355.53|0.8%|3|
|false|True|True|8|True|306787|1.738|—/166.40|1.3%|3|
|false|True|True|16|False|157383|0.892|—/350.41|2.8%|3|
|false|True|True|16|True|300139|1.700|—/167.12|1.7%|3|
|true|False|False|0|None|555173|1.000|—/104.67|0.1%|3|
|true|False|False|1|False|511062|0.921|—/120.58|0.7%|3|
|true|False|False|1|True|617617|1.112|—/68.87|0.5%|3|
|true|False|True|0|None|497284|1.000|—/109.42|0.3%|3|
|true|False|True|1|False|514847|1.035|—/120.09|0.3%|3|
|true|False|True|1|True|616861|1.240|—/69.35|0.1%|3|
|true|False|True|2|False|542168|1.090|—/154.15|0.3%|3|
|true|False|True|2|True|964314|1.939|—/70.00|0.2%|3|
|true|False|True|4|False|488886|0.983|—/163.06|0.2%|3|
|true|False|True|4|True|920741|1.852|—/74.86|0.8%|3|
|true|False|True|8|False|452291|0.910|—/166.32|0.6%|3|
|true|False|True|8|True|775827|1.560|—/100.48|0.4%|3|
|true|False|True|16|False|451052|0.907|—/166.26|0.5%|3|
|true|False|True|16|True|777748|1.564|—/101.70|0.4%|3|
|true|True|False|0|None|217969|1.000|—/245.68|6.5%|3|
|true|True|False|1|False|156981|0.720|—/358.37|2.1%|3|
|true|True|False|1|True|307939|1.413|—/166.50|1.2%|3|
|true|True|True|0|None|222516|1.000|—/215.32|9.6%|3|
|true|True|True|1|False|152343|0.685|—/369.23|3.1%|3|
|true|True|True|1|True|302893|1.361|—/167.24|2.3%|3|
|true|True|True|2|False|159770|0.718|—/336.29|3.8%|3|
|true|True|True|2|True|284022|1.276|—/169.06|6.6%|3|
|true|True|True|4|False|161227|0.725|—/342.64|0.3%|3|
|true|True|True|4|True|300733|1.352|—/167.54|6.1%|3|
|true|True|True|8|False|159288|0.716|—/346.98|2.5%|3|
|true|True|True|8|True|303850|1.366|—/166.83|2.5%|3|
|true|True|True|16|False|158515|0.712|—/347.93|0.8%|3|
|true|True|True|16|True|308135|1.385|—/166.70|1.0%|3|
