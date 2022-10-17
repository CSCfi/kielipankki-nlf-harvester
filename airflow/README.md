## Run a simple download DAG in Airflow locally in Docker

---

### Requirements

- Docker installation

### How to

1. Open terminal and navigate to `airflow/` folder

2. Add user ID to .env with the following command:

    ```echo -e "AIRFLOW_UID=$(id -u)" > .env```

3. Initialize Airflow in docker:

    ```docker compose up airflow-init```

4. Build image and run container.

    ```docker compose up --build```

5. Wait for the container to launch, and open Airflow in `http://0.0.0.0:8080/`

6. Turn on the DAG `download_altos_for_binding` and inspect it if you wish.

&nbsp;

`./mets_folder` should now have one METS file and `./alto_folder` four ALTO files identified from the downloaded METS.
