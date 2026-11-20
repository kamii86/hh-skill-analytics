# Kubernetes: расчёт пар навыков

Нугаева Камилла, ИСУ 434674, группа J3312. Вариант №9: разработчики мобильных платформ.

В работе используется программа ДЗ №3 из `src/mapreduce_skills.py` и исходный объект `s3://hh-lake/raw/Raw_Jobs.csv`.

## Результаты

Эксперименты выполнены на локальном Minikube с Docker на macOS ARM64. Kubernetes 1.37.0, один узел `hh-k8s`, containerd 2.3.4.

| Проверка | Результат |
|---|---|
| Профилирование памяти | Пик cgroup: 101,19 MiB при лимите 1024Mi |
| Нехватка памяти | Лимит 64Mi: OOMKilled, exit code 137 |
| Подобранный предел | 128Mi по первому пику с 25% запаса и округлением до 16Mi; повтор завершился успешно |
| Последующий одиночный расчёт из S3 кластера | Пик 110,93 MiB; Completed при лимите 128Mi |
| Перенос RustFS | Deployment, Service и PVC 15Gi; SHA-256 источника и копии совпали |
| Параллельный расчёт | 4 части, Indexed Job, одновременно до 2 Pod |
| Сверка с ДЗ №3 | Все 3 674 пары и их частоты совпали; итоговый CSV совпал побайтово |
| Ожидание CPU | Pending с Insufficient cpu; после освобождения CPU тот же Pod запущен |
| Вытеснение | hh-low (-1000) вытеснен задачей hh-high (-500); событие связано с UID обеих задач |
| Квота | Второй запрос 300m отклонён: 300m + 300m > 500m |

Полный конвейер с разбиением и агрегацией занял 89 с, одиночный Job — 17 с. Ускорения на этом запуске не получено.

## Файлы

- `Dockerfile` — образ Python-программы.
- `k8s/workload.py` — запуск исходного mapper/reducer, разделение CSV и сверка результатов.
- `k8s/lab.py` — проведение экспериментов и сохранение доказательств.
- `k8s/manifests/` — RustFS, PVC, Service, приоритеты и квота.
- `k8s/baseline/pairs.csv` — эталон ДЗ №3.
- `k8s/runs/20261119T230458Z/` — журналы, фактические манифесты, состояния Kubernetes и результаты.
- `reports/Nugaeva_Kubernetes_Report.pdf` — отчёт.

В каталоге запуска ключевые результаты находятся в `memory-summary.json`, `storage/source-copy.json`, `final/verification.json`, `scheduling/preemption-proof.json` и `quota/rejection.txt`.

Первая неудачная попытка проверки вытеснения сохранена в `scheduling-attempts/`. Итоговое подтверждение находится в `scheduling/PASS.json` и относится к исправленной попытке с отрицательными приоритетами.

## Воспроизведение

Нужны Docker Desktop, Minikube, kubectl и доступ к исходному RustFS. В `.env` задаются S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET и SOURCE_KEY. Файл `.env` не публикуется.

```bash
minikube start -p hh-k8s --driver=docker --cpus=2 --memory=3072
kubectl --context hh-k8s wait node/hh-k8s --for=condition=Ready --timeout=180s

python3 -m venv .venv-k8s
source .venv-k8s/bin/activate
python -m pip install -r k8s/requirements.txt
python -m unittest discover -s k8s/tests -v

python -m src.check_s3
python -m k8s.lab setup
python -m k8s.lab memory
python -m k8s.lab storage
python -m k8s.lab pipeline --parts 4
python -m k8s.lab scheduling
```

Команды выполняются по порядку; при ошибке следующий этап не запускается. На Mac старый S3 должен быть доступен контейнерам по `http://host.docker.internal:9000`; другой адрес указывается через `setup --external-endpoint`.

В сохранённом запуске Kubernetes показывал `allocatable.cpu=8000m`. Заявки тестовых Pod рассчитаны по фактическому значению API: 1050m уже занято, 6850m резервирует держатель, 100m остаётся свободным.

Разделение выполняется CSV-парсером. Одинаковые непустые ID отправляются в одну часть по SHA-256, порядок внутри части сохраняется. Поэтому удаление повторных ID даёт тот же результат, что последовательный расчёт ДЗ №3.

Старый RustFS не удаляется. Учебный RustFS внутри кластера использует один PVC; отказоустойчивость четырёх независимых дисков эта конфигурация не воспроизводит.
