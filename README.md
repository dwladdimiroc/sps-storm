# Self-adaptive in Apache Storm
Implementation of MAPE Model for Storm extension in the article [[1]](#1) and [[2]](#2). This project presents the self-adaptive system for to modify the number of active/inactive replicas for each pool of operators in the SPS application. The goal is to analyse differences metrics (i.e. input, executed time, queue) and to plan the changes necessary for to process all input events in the SPS.   

## Configuration
The config file '[config.yaml](configs/config.yaml)' has three principals parameters: `nimbus`, `redis`, `storm`. 

The parameter `nimbus` is related to Nimbus component in Storm. The variables `host` and `port` are the IP location of Nimbus.

The parameter `redis` is related to Redis cache. The variables `host` and `port` are the IP location of Redis.

The parameter `predictor` is related to Predictor API. The variables `host` and `port` are the IP location of Predictor API.

The params `storm` is related to Apache Storm.

The variable `deploy` is related to application deployment. 
- `duration` is the time of the experiment
- `script` is the app script that Apache will deploy
- `analyze` is the parameters if the system adapts (or not) the Storm application.  

The variable `adaptive` is related to self-adaptive system.
- `time_window_size` size of the time period where a sample is obtained. Its value is in seconds.
- `benchmark_samples` numbers of samples used by the benchmark.
- `analyze_samples` numbers of samples by MAPE model.
- `preditive_model` model used by input prediction. it's possible variables: `basic`, `regression_linear`, `fft`, `ann`, `random_forest`
- `prediction_samples`  number of samples used by predictive model
- `prediction_number`  number of predictions made by predictive model
- `limit_repicas`  limit of number of pool replicas

The variable `csv` is the folder where the system saves the statistics.

## Requisites
For compile this project you need `go` and `redis`, and of course, `storm`. Please refer to you platform's/OS' documentation for support.

The `go` last version used was 1.91.1  (see the <a href="https://go.dev/doc/install">go installation instructions</a> for details). For `redis`, the last version used was 6.x. And `storm`, you must use the `1.x` version (see <a href="https://github.com/apache/storm/tree/1.x-branch">storm branch</a>).

## Deploy

The main file is `initSps.sh` which is responsible for run the monitor. If the machine has no Golang installed, so you should comment line 4 `go build`, because this linea compile again the Go project. It's mandatory create the `\stats` folder in the project. And the `scripts` folder has Storm applications that the system can use. Each script is the commands for deploy Storm app, so you must change the Storm directory is necessary.  

## References
<a id="1" href="https://hal.science/hal-03962939/file/SBAC_PAD_2022___Paper___A_predictive_approach_for_dynamic_replication_of_operators_in_distributed_stream_processing_systems.pdf">[1]</a>
D. Wladdimiro, L. Arantes, P. Sens and N. Hidalgo. (2022). 
A predictive approach for dynamic replication of operators in distributed stream processing systems.
IEEE 34th International Symposium on Computer Architecture and High Performance Computing (SBAC-PAD), Bordeaux, France, pp. 120-129.

<a id="2" href="https://hal.science/hal-03783768/file/ComPAS2022_paper_22-2.pdf">[2]</a>
Daniel Wladdimiro, Luciana Arantes, Nicolas Hidalgo, Pierre Sens. (2022)
A predictive model for Stream Processing System that dynamically calibrates the number of operator replicas.
Conférence francophone d'informatique en Parallélisme, Architecture et Système (ComPAS), Amiens, France. 