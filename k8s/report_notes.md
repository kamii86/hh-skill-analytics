# Материалы для отчёта

Нугаева Камилла, ИСУ 434674, J3312. Вариант №9 — мобильные разработчики.

Эксперименты завершены; журналы сохранены в `k8s/runs/20261119T230458Z/`. Итоговый отчёт: `reports/Nugaeva_Kubernetes_Report.pdf`. Ниже приведены пояснения к реализации и сравнению систем.

## Подтверждения результатов

| Требование | Файл подтверждения |
|---|---|
| Кластер и собственный образ | `setup/`, Pod imageID, Dockerfile, хеши исходников |
| Остановка по памяти | JSON Pod: `reason=OOMKilled`, лимит, exitCode и журнал |
| Пик и выбранный лимит | `memory-summary.json`: cgroup peak, формула 25% запаса и успешный повтор |
| S3 внутри Kubernetes | Deployment, Service, PVC, `storage/source-copy.json` с двумя SHA-256 |
| Параллельные части | Indexed Job, его Pod и время startedAt/finishedAt, manifest частей |
| Результат ДЗ №3 | Полный `pairs.csv`, `verification.json`, таблица топ-10 |
| Недостаток CPU | `pending.json`, условие `Insufficient cpu`, Running после освобождения |
| Вытеснение | `low-before.json`, события Preempted, снимки и Running высокого Pod |
| Квота | ResourceQuota used/hard до/после, отказ API по requests.cpu |

Нельзя делать вывод об ускорении только по тому, что работали два Pod.
Для этого нужны сопоставимые фактические времена, включая чтение S3 и отдельно накладные расходы разбиения и агрегации.

## Соответствие Kubernetes, YARN и Mesos

Это функциональные соответствия: объекты и полномочия систем не совпадают один к одному.

| Функция | YARN | Mesos | Kubernetes |
|---|---|---|---|
| Распределение ресурсов кластера | ResourceManager и его Scheduler | Master/allocator и планировщики frameworks | kube-scheduler; желаемое состояние хранится через API |
| Агент узла | NodeManager | Mesos agent | kubelet и container runtime |
| Управление конкретным приложением | ApplicationMaster | Framework scheduler и executors | Контроллер Job; в нашей работе host-runner задаёт последовательность стадий |
| Запускаемая работа | YARN container с выделенными ресурсами | Task, выполняемый executor | Pod с контейнером; Job управляет завершением и повторами |
| Механизм планирования | AM запрашивает ресурсы, RM распределяет containers | Master предлагает ресурсы frameworks; их schedulers принимают или отклоняют offers | Scheduler фильтрует и ранжирует узлы по требованиям Pod |
| Разделение между пользователями | Очереди scheduler, их ёмкости и политика | Roles, weights, reservations, quota | Namespace, ResourceQuota, PriorityClass и admission |

### Где аналог ApplicationMaster

Прямого встроенного эквивалента YARN ApplicationMaster в Kubernetes нет.
Job controller поддерживает число Pod и отслеживает завершение.
Он не знает о семантике Map/Reduce и не строит произвольный DAG заданий.
В этой работе последовательность split → workers → reduce задаёт `k8s.lab`,
а запуск и размещение каждого Pod выполняют стандартные компоненты Kubernetes.

### Почему YARN container не равен Docker container

В YARN container — выделенные приложению ресурсы и контекст запуска процесса на узле.
Это понятие само по себе не требует Docker-образа.
В Kubernetes минимальная единица планирования — Pod: он может содержать несколько контейнеров.
В работе каждый вычислительный Pod содержит один Python-контейнер.

### Отличие двухуровневого планирования Mesos

Mesos allocator выбирает, каким frameworks предложить ресурсы.
Framework scheduler решает, принять ли offer и какие задачи на этих ресурсах запустить.
В стандартном Kubernetes приложение публикует требования Pod в API,
а kube-scheduler сам выбирает узел, не предлагая приложению список доступных ресурсов.

### Requests, limits и фактическая загрузка

Scheduler проверяет CPU и память по requests, а не по мгновенному проценту загрузки узла.
Поэтому Pod может ждать с `Insufficient cpu`, хотя `top` показывает небольшой расход CPU.
CPU limit ограничивает процессорное время через throttling; превышение само по себе не означает OOMKilled.
Memory limit применяется через cgroups: нехватка памяти в cgroup может завершить процесс, что фиксируется как OOMKilled.
Запрос памяти не равен реальному пику её потребления и не заменяет его измерение.

### Вытеснение и квота решают разные задачи

PriorityClass определяет приоритет Pod. При нехватке ресурсов scheduler может
вытеснить Pod меньшего приоритета, если это позволит разместить ожидающий Pod.
Это не приостановка с сохранением состояния процесса: жертва завершается.
Контроллер может создать ей замену, но в нашем опыте используется отдельный Pod без контроллера.

ResourceQuota ограничивает суммарное потребление заявленных ресурсов в namespace.
В нашей конфигурации это верхний бюджет requests.cpu, а не гарантия доступности CPU.
Если бюджет превышен, API отклоняет создание Pod ещё до размещения.
Высокий PriorityClass не позволяет обойти ResourceQuota.

Очередь YARN не следует считать полным аналогом namespace с ResourceQuota:
очереди участвуют в распределении ёмкости и политике scheduler,
а стандартная ResourceQuota в первую очередь проверяет бюджет при admission.
Аналогично quota в Mesos может задавать гарантируемую долю роли;
её нельзя без оговорок приравнивать к верхнему лимиту namespace.

### Данные и вычисления

Kubernetes не выполняет MapReduce-агрегацию автоматически.
Python-код формирует пары, S3 хранит источник, части и результаты,
Indexed Job распределяет части между Pod, а Reduce Job суммирует счётчики.
PVC обеспечивает сохранность данных RustFS при пересоздании его Pod.
Один PVC на одном узле не даёт защиты от отказа диска или всего компьютера.

## Источники

- YARN architecture: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
- YARN applications: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
- Mesos architecture: https://mesos.apache.org/documentation/latest/architecture/
- Mesos allocation: https://mesos.apache.org/documentation/latest/allocation-module/
- Mesos framework scheduling and roles: https://mesos.apache.org/documentation/app-framework-development-guide/
- Kubernetes resource management: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
- Kubernetes priority: https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/
- Kubernetes quota: https://kubernetes.io/docs/concepts/policy/resource-quotas/
