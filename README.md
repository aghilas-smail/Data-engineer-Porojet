# Data-engineer-Porojet

1. Streaming de données : dans un premier temps, les données sont diffusées depuis l’API vers une rubrique Kafka.
2. Traitement des données : une tâche Spark prend ensuite le relais, consommant les données de la rubrique Kafka et les transférant vers une base de données PostgreSQL.
3. Planification avec Airflow : la tâche de streaming et la tâche Spark sont toutes deux orchestrées à l’aide d’Airflow. Alors que dans un scénario réel, le producteur Kafka écouterait constamment l’API, à des fins de démonstration, nous allons planifier l’exécution quotidienne de la tâche de streaming Kafka. Une fois le streaming terminé, la tâche Spark traite les données, les rendant prêtes à être utilisées par l’application LLM.

Tous ces outils seront créés et exécutés à l’aide de Docker, et plus précisément de [docker-compose](https://docs.docker.com/compose/).
