# Apache Airflow Docker Project

## Overview

This project provides a Dockerized setup for Apache Airflow, utilizing a CeleryExecutor with Redis and PostgreSQL. It is designed for local development and should not be used in a production environment without further modifications.

## Prerequisites

- Docker
- Docker Compose

## Project Structure

The project is organized as follows:
.
├── dags
│ └── [DAG files here]
├── logs
│ └── [Airflow logs]
├── scripts
│ └── [Python scripts for data processing]
├── data
│ └── [Data files]
├── docker-compose.yaml
└── .env

## Environment Variables

For the .env in /dags, replace these lines if needed:

`**MONGO\_CONNECTION\_STRING="mongodb+srv://facciadmin:**RJ87M6a1lNYdn90@facci-stylematch-db.mongocluster.c**osmos.azure.com/facci-stylematch?tls=true&authMech**anism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeM**S=120000"**`

`**AZURE\_STORAGE\_CONNECTION\_STRING="DefaultEndpointsP**rotocol=https;AccountName=stylematchblob;AccountKe**y=CvAuoWrOv8NorYuVfEFIP+PSu17HZA15Kjzqq5OxQXA+tRdx**JxJ3TqEA/0oSohrmN8rFFUzIL3vn+ASt8WbFtg==;EndpointS**uffix=core.windows.net"**`

## Services

The `docker-compose.yaml` file defines the following services:

- **PostgreSQL**: Database for Airflow metadata.
- **Redis**: Message broker for Celery.
- **Airflow Webserver**: Web interface for Airflow.
- **Airflow Scheduler**: Schedules tasks.
- **Airflow Workers**: Execute tasks.
- **Airflow Triggerer**: Triggers tasks based on external events.
- **Flower**: Real-time monitoring tool for Celery.

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/YourUsername/YourRepo.git
   cd YourRepo
   ```
2. Create a `.env` file based on the provided example or modify the existing one to suit your needs.
3. Start the services:

   ```bash
   docker-compose up
   ```
4. Access the Airflow web interface at `http://localhost:8080` using the credentials specified in the `.env` file.

## Running Scripts

Scripts for data processing can be found in the `scripts` directory. You can run these scripts within the Airflow environment or directly using Docker.

## Health Checks

The services include health checks to ensure they are running correctly. You can monitor the health of the services through the logs or by accessing the respective ports.

## Conclusion

This Dockerized setup for Apache Airflow provides a flexible and scalable environment for orchestrating data workflows. Modify the configuration as needed to fit your specific use case.
