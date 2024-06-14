# Serverless App
This project presents the serverless solution in GCP. Bolts process synthetic events that are created by the Spout according to a distribution. 

## Setting
To deploy the application, use two parameters: `distribution` and `pool_size`.
The available distributions are:
- `base`: The number of events is 2000 per second
- `constant`: The number of events is 10 per second
- `dns`: The DNS trace used in [[1]](#1)
- `exponential`: The number of events is increasing according to an exponential function
- `lineal`: Linear function that starts from 0 to 1000 and restarts the counter
- `log`: The distributed file logs trace used in [[2]](#2)
- `logarithm`: The number of events is increasing according to a logarithm function
- `n-norm`: Distribution of events based on a normal distribution n times
- `norm`: Distribution of events based on a normal distribution
- `poisson`: Distribution of events based on a poisson distribution
- `twitter`: Smoothed Twitter trace used [[3]](#3)
- `twitter-raw`: Raw Twitter trace used [[3]](#3)

<a id="1" href="https://ieeexplore.ieee.org/abstract/document/9251211/">[1]</a>
MontazeriShatoori, M., Davidson, L., Kaur, G., & Lashkari, A. H. (2020, August). Detection of doh tunnels using time-series classification of encrypted traffic. In 2020 IEEE Intl Conf on Dependable, Autonomic and Secure Computing, Intl Conf on Pervasive Intelligence and Computing, Intl Conf on Cloud and Big Data Computing, Intl Conf on Cyber Science and Technology Congress (DASC/PiCom/CBDCom/CyberSciTech) (pp. 63-70). IEEE.

<a id="2" href="https://ieeexplore.ieee.org/abstract/document/10301257/">[2]</a>
Zhu, J., He, S., He, P., Liu, J., & Lyu, M. R. (2023, October). Loghub: A large collection of system log datasets for ai-driven log analytics. In 2023 IEEE 34th International Symposium on Software Reliability Engineering (ISSRE) (pp. 355-366). IEEE.

<a id="3" href="https://ieeexplore.ieee.org/abstract/document/9981007/">[3]</a>
Wladdimiro, D., Arantes, L., Sens, P., & Hidalgo, N. (2022, November). A predictive approach for dynamic replication of operators in distributed stream processing systems. In 2022 IEEE 34th International Symposium on Computer Architecture and High Performance Computing (SBAC-PAD) (pp. 120-129). IEEE.