# NYC Yellow Cab Analytics Platform

Une plateforme complète d'analyse de données pour les taxis jaunes de New York City, construite avec une architecture moderne de données : ingestion, transformation, machine learning et visualisation.

---

## Table des matières

1. [Architecture Technique](#1-architecture-technique)
2. [Pipelines de Données](#2-pipelines-de-données)
3. [DAGs Airflow et Modèles dbt](#3-dags-airflow-et-modèles-dbt)
4. [Machine Learning avec MLflow](#4-machine-learning-avec-mlflow)
5. [Interface de Visualisation](#5-interface-de-visualisation)

---

## 1. Architecture Technique

### Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         NYC Yellow Cab Analytics Platform                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   NYC TLC    │     │   Airflow    │     │   Django     │                │
│  │  Open Data   │────▶│ Orchestrator │────▶│  Dashboard   │                │
│  └──────────────┘     └──────┬───────┘     └──────────────┘                │
│                              │                     ▲                        │
│                              ▼                     │                        │
│  ┌──────────────┐     ┌──────────────┐     ┌──────┴───────┐                │
│  │    dbt       │◀────│  BigQuery    │────▶│   MLflow     │                │
│  │  Transform   │────▶│   DWH        │     │  Tracking    │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│                              ▲                                              │
│                              │                                              │
│                       ┌──────┴───────┐                                      │
│                       │ Cloud Storage│                                      │
│                       │   (Bronze)   │                                      │
│                       └──────────────┘                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Composants

#### Apache Airflow - L'Orchestrateur

Airflow est le chef d'orchestre de notre plateforme. Il planifie et exécute les différentes tâches dans le bon ordre, gère les dépendances entre elles, et permet de monitorer l'ensemble du pipeline.

**Pourquoi Airflow ?**
- Définition des workflows en Python (DAGs)
- Interface web pour le monitoring
- Gestion des retries et des alertes
- Scalabilité avec Celery Workers

**Configuration utilisée :**
```yaml
Executor: CeleryExecutor
Workers: Conteneurs Docker
Base de données: PostgreSQL
Broker: Redis
```

#### dbt (Data Build Tool) - Le Transformateur

dbt permet de transformer les données brutes en données analytiques de qualité. Il applique le principe du "ELT" (Extract, Load, Transform) où les transformations se font directement dans le Data Warehouse.

**Pourquoi dbt ?**
- SQL versionné et testé
- Documentation automatique
- Lignage des données
- Tests de qualité intégrés

**Structure du projet dbt :**
```
dbt/nyc_taxi/
├── models/
│   ├── staging/          # Nettoyage initial
│   ├── intermediate/     # Transformations métier
│   └── marts/            # Tables finales (faits/dimensions)
├── tests/                # Tests de qualité
└── macros/               # Fonctions réutilisables
```

#### Django - L'Interface Web

Django propulse le dashboard de visualisation. Ce framework Python robuste permet de créer rapidement des applications web avec une architecture MVC claire.

**Pourquoi Django ?**
- Framework mature et sécurisé
- ORM puissant (même si on utilise BigQuery directement ici)
- Système de templates flexible
- Écosystème riche

#### MLflow - Le Tracker ML

MLflow gère le cycle de vie des modèles de machine learning : expérimentation, versioning, et déploiement.

**Pourquoi MLflow ?**
- Tracking des expériences (paramètres, métriques)
- Stockage des artefacts (modèles, graphiques)
- Model Registry pour le versioning
- Interface de comparaison

#### Google BigQuery - Le Data Warehouse

BigQuery est notre entrepôt de données serverless. Il stocke et analyse des volumes massifs de données avec des performances exceptionnelles.

**Organisation des datasets :**
```
project/
├── nyc_taxi_bronze/    # Données brutes (via External Tables)
├── nyc_taxi_silver/    # Données nettoyées (dbt)
└── nyc_taxi_gold/      # Agrégations business (Airflow)
```

#### Google Cloud Storage - Le Stockage Brut

GCS stocke les fichiers Parquet bruts téléchargés depuis NYC TLC. C'est notre couche "Bronze" dans l'architecture Medallion.

---

## 2. Pipelines de Données

### Source des Données

Les données proviennent du **NYC Taxi & Limousine Commission (TLC)**, l'autorité qui régule les taxis et VTC à New York City.

**URL source :** `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

**Format :** Fichiers Parquet mensuels (~100 Mo chacun)

**Contenu d'un enregistrement :**
| Champ | Description |
|-------|-------------|
| `VendorID` | Fournisseur du système de collecte |
| `tpep_pickup_datetime` | Date/heure de prise en charge |
| `tpep_dropoff_datetime` | Date/heure de dépose |
| `passenger_count` | Nombre de passagers |
| `trip_distance` | Distance en miles |
| `PULocationID` | Zone de prise en charge |
| `DOLocationID` | Zone de dépose |
| `payment_type` | Mode de paiement |
| `fare_amount` | Tarif de base |
| `tip_amount` | Pourboire |
| `total_amount` | Montant total |

### Architecture Medallion

Notre pipeline suit l'architecture **Medallion** (Bronze → Silver → Gold) :

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │     │   SILVER    │     │    GOLD     │
│             │     │             │     │             │
│ Données     │────▶│ Données     │────▶│ Agrégations │
│ brutes      │     │ nettoyées   │     │ business    │
│             │     │             │     │             │
│ • Parquet   │     │ • Typage    │     │ • KPIs      │
│ • Tel quel  │     │ • Validation│     │ • Tendances │
│             │     │ • Enrichi   │     │ • Analytics │
└─────────────┘     └─────────────┘     └─────────────┘
     GCS                BigQuery           BigQuery
```

**Bronze (Cloud Storage)**
- Fichiers Parquet bruts
- Aucune transformation
- Conservation de l'historique complet

**Silver (BigQuery - dbt)**
- Typage correct des colonnes
- Validation des données (distances, durées, montants)
- Enrichissement (calcul de vitesse, flags de qualité)
- Modèle dimensionnel (faits + dimensions)

**Gold (BigQuery - Airflow)**
- Agrégations pré-calculées
- Tables optimisées pour le dashboard
- Métriques business prêtes à l'emploi

### Principe des DAGs Airflow

Un **DAG** (Directed Acyclic Graph) représente un workflow comme un graphe de tâches avec des dépendances.

```python
# Exemple simplifié d'un DAG
with DAG("exemple_dag", schedule="@daily") as dag:

    tache_1 = extraire_donnees()
    tache_2 = transformer_donnees()
    tache_3 = charger_donnees()

    tache_1 >> tache_2 >> tache_3
    #    │         │         │
    #    └─────────┴─────────┘
    #      Ordre d'exécution
```

**Caractéristiques clés :**
- **Directed** : Les tâches ont un sens (A → B)
- **Acyclic** : Pas de boucles (pas de A → B → A)
- **Graph** : Structure de nœuds et d'arêtes

---

## 3. DAGs Airflow et Modèles dbt

### Vue d'ensemble des DAGs

```
┌─────────────────────────────────────────────────────────────────┐
│                        Orchestration Airflow                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐                                            │
│  │ Bronze Ingestion│  @monthly                                  │
│  │    (DAG 1)      │──────────┐                                 │
│  └─────────────────┘          │                                 │
│                               ▼                                 │
│  ┌─────────────────┐   ┌─────────────────┐                     │
│  │  Silver dbt     │   │ Gold Aggregation│                     │
│  │    (DAG 2)      │──▶│    (DAG 3)      │                     │
│  └─────────────────┘   └────────┬────────┘                     │
│                                 │                               │
│                                 ▼                               │
│                        ┌─────────────────┐                     │
│                        │ ML Forecasting  │  @monthly           │
│                        │    (DAG 4)      │                     │
│                        └─────────────────┘                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### DAG 1 : Bronze Ingestion

**Fichier :** `dags/nyc_taxi_bronze_ingestion.py`

**Objectif :** Télécharger les données mensuelles depuis NYC TLC et les stocker dans Cloud Storage.

**Schedule :** `@monthly` (le 1er de chaque mois)

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ generate_url     │────▶│ download_upload  │────▶│ validate_upload  │
│                  │     │                  │     │                  │
│ Génère l'URL du  │     │ Télécharge et    │     │ Vérifie le       │
│ fichier Parquet  │     │ upload vers GCS  │     │ fichier uploadé  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
                                                           │
                                                           ▼
                                                  ┌──────────────────┐
                                                  │ log_metrics      │
                                                  │                  │
                                                  │ Log les stats    │
                                                  │ d'ingestion      │
                                                  └──────────────────┘
```

**Détail des tâches :**

| Tâche | Description |
|-------|-------------|
| `generate_download_url` | Construit l'URL TLC pour le mois à traiter |
| `download_and_upload_to_gcs` | Stream le fichier vers Cloud Storage |
| `validate_upload` | Vérifie la taille et le nombre de colonnes |
| `log_ingestion_metrics` | Affiche un résumé de l'ingestion |

### DAG 2 : Silver dbt Transformation

**Fichier :** `dags/nyc_taxi_silver_dbt.py`

**Objectif :** Exécuter les transformations dbt pour créer la couche Silver.

**Schedule :** `@daily`

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ dbt_deps         │────▶│ dbt_run          │────▶│ dbt_test         │
│                  │     │                  │     │                  │
│ Installe les     │     │ Exécute les      │     │ Lance les tests  │
│ packages dbt     │     │ modèles          │     │ de qualité       │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

### Modèles dbt en détail

#### Couche Staging

**`stg_yellow_trips.sql`** - Nettoyage initial des données brutes

```sql
-- Exemple simplifié
SELECT
    -- Identifiants
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as trip_id,

    -- Timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) as dropoff_datetime,

    -- Métriques nettoyées
    CASE
        WHEN trip_distance < 0 THEN 0
        WHEN trip_distance > 500 THEN NULL
        ELSE trip_distance
    END as trip_distance_miles,

    -- Flag de qualité
    CASE
        WHEN trip_distance <= 0 OR total_amount <= 0 THEN TRUE
        ELSE FALSE
    END as has_data_quality_issue

FROM {{ source('bronze', 'yellow_tripdata') }}
```

#### Couche Marts (Faits et Dimensions)

**Modèle de données dimensionnel :**

```
                    ┌─────────────────┐
                    │   dim_vendors   │
                    │─────────────────│
                    │ vendor_id (PK)  │
                    │ vendor_name     │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│ dim_payment_    │          │          │  dim_locations  │
│     types       │          │          │─────────────────│
│─────────────────│          │          │ location_id (PK)│
│ payment_id (PK) │          │          │ borough         │
│ payment_name    │          │          │ zone            │
└────────┬────────┘          │          │ service_zone    │
         │                   │          └────────┬────────┘
         │     ┌─────────────┴──────────────┐    │
         │     │        fct_trips           │    │
         └────▶│────────────────────────────│◀───┘
               │ trip_id (PK)               │
               │ vendor_id (FK)             │
               │ payment_type_id (FK)       │
               │ pickup_location_id (FK)    │
               │ dropoff_location_id (FK)   │
               │ pickup_datetime            │
               │ trip_distance_miles        │
               │ trip_duration_minutes      │
               │ total_amount               │
               │ tip_amount                 │
               │ avg_speed_mph              │
               │ has_data_quality_issue     │
               └────────────────────────────┘
                             │
                             ▼
               ┌─────────────────────────────┐
               │       dim_dates            │
               │─────────────────────────────│
               │ date_key (PK)              │
               │ full_date                  │
               │ year, month, day           │
               │ day_of_week, day_name      │
               │ is_weekend, is_holiday     │
               └─────────────────────────────┘
```

### DAG 3 : Gold Aggregations

**Fichier :** `dags/nyc_taxi_gold_aggregations.py`

**Objectif :** Créer les tables agrégées optimisées pour le dashboard.

**Schedule :** `@daily`

```
                        ┌──────────────────┐
                        │ create_dataset   │
                        │                  │
                        │ Crée le dataset  │
                        │ GOLD si absent   │
                        └────────┬─────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │           │          │          │           │
          ▼           ▼          ▼          ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ daily_   │ │ hourly_  │ │ monthly_ │ │ payment_ │ │ vendor_  │
    │ summary  │ │ patterns │ │ trends   │ │ analysis │ │ compare  │
    └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
         │            │            │            │            │
         └────────────┴────────────┴────────────┴────────────┘
                                   │
                                   ▼
                        ┌──────────────────┐
                        │ log_completion   │
                        └──────────────────┘
```

**Tables Gold créées :**

| Table | Description | Granularité |
|-------|-------------|-------------|
| `daily_summary` | Résumé quotidien des trips | 1 ligne/jour |
| `hourly_patterns` | Patterns par heure/jour semaine | 168 lignes (24h × 7j) |
| `monthly_trends` | Tendances mensuelles | 1 ligne/mois |
| `location_stats` | Stats par zone taxi | ~260 lignes |
| `payment_analysis` | Analyse par mode paiement | Par mois × type |
| `vendor_comparison` | Comparaison vendeurs | Par mois × vendeur |
| `year_over_year` | Comparaison annuelle | 1 ligne/an |
| `trip_distance_distribution` | Distribution distances | 6 buckets |

---

## 4. Machine Learning avec MLflow

### DAG ML Forecasting

**Fichier :** `dags/nyc_taxi_ml_forecasting.py`

**Objectif :** Entraîner des modèles de prévision de demande et les tracker dans MLflow.

**Schedule :** `@monthly`

### Architecture du Pipeline ML

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          ML Forecasting Pipeline                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                   │
│  │ setup_      │     │ fetch_      │     │ extract_    │                   │
│  │ mlflow      │────▶│ training_   │────▶│ params      │                   │
│  │             │     │ data        │     │             │                   │
│  │ Crée        │     │             │     │ Récupère    │                   │
│  │ l'experiment│     │ BigQuery    │     │ les params  │                   │
│  └─────────────┘     │ → DataFrame │     │ du DAG      │                   │
│                      └─────────────┘     └──────┬──────┘                   │
│                                                 │                          │
│                                                 ▼                          │
│                                    ┌────────────────────┐                  │
│                                    │    train_model     │                  │
│                                    │                    │                  │
│                                    │ • Prophet          │                  │
│                                    │ • ARIMA            │                  │
│                                    │ • XGBoost          │                  │
│                                    │ • LightGBM         │                  │
│                                    │ • Holt-Winters     │                  │
│                                    └─────────┬──────────┘                  │
│                                              │                             │
│                          ┌───────────────────┼───────────────────┐         │
│                          ▼                   │                   ▼         │
│               ┌─────────────────┐            │        ┌─────────────────┐  │
│               │ generate_       │            │        │ register_       │  │
│               │ future_forecast │            │        │ model           │  │
│               │                 │            │        │                 │  │
│               │ Prévisions      │            │        │ MLflow Model    │  │
│               │ N jours         │            │        │ Registry        │  │
│               └────────┬────────┘            │        └────────┬────────┘  │
│                        │                     │                 │           │
│                        └─────────────────────┴─────────────────┘           │
│                                              │                             │
│                                              ▼                             │
│                                    ┌─────────────────┐                     │
│                                    │ log_completion  │                     │
│                                    └─────────────────┘                     │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

### Modèles Disponibles

Le pipeline supporte 5 algorithmes de forecasting :

| Modèle | Type | Points forts | Cas d'usage |
|--------|------|--------------|-------------|
| **Prophet** | Additif | Saisonnalités multiples, robuste aux outliers | Données avec tendances et saisonnalités claires |
| **SARIMA** | Statistique | Interprétable, pas de tuning | Séries stationnaires |
| **XGBoost** | Gradient Boosting | Capture relations non-linéaires | Features temporelles riches |
| **LightGBM** | Gradient Boosting | Rapide, efficace en mémoire | Grands volumes |
| **Holt-Winters** | Lissage exponentiel | Simple, robuste | Saisonnalité régulière |

### Paramètres du DAG

Le DAG expose des paramètres configurables via l'interface Airflow :

```python
params = {
    "model_type": "prophet",           # Choix du modèle
    "forecast_horizon_days": 30,       # Jours à prédire
    "training_years": 2,               # Années d'historique
    "target_metric": "total_trips",    # Variable cible
    "register_model": False,           # Enregistrer dans Registry
}
```

### Features Temporelles (XGBoost/LightGBM)

Pour les modèles de gradient boosting, des features sont créées automatiquement :

```python
def create_time_features(df):
    df['year'] = df['ds'].dt.year
    df['month'] = df['ds'].dt.month
    df['day'] = df['ds'].dt.day
    df['dayofweek'] = df['ds'].dt.dayofweek
    df['dayofyear'] = df['ds'].dt.dayofyear
    df['weekofyear'] = df['ds'].dt.isocalendar().week
    df['quarter'] = df['ds'].dt.quarter
    df['is_weekend'] = df['ds'].dt.dayofweek.isin([5, 6])
    df['is_month_start'] = df['ds'].dt.is_month_start
    df['is_month_end'] = df['ds'].dt.is_month_end

    # Encodage cyclique pour capturer la périodicité
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['day_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
    df['day_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)

    return df
```

### Métriques Trackées dans MLflow

Chaque run enregistre automatiquement :

**Paramètres :**
- `model_type`, `target_metric`
- `forecast_horizon_days`, `training_samples`
- `date_range_start`, `date_range_end`

**Métriques :**
- `MAE` (Mean Absolute Error)
- `RMSE` (Root Mean Square Error)
- `MAPE` (Mean Absolute Percentage Error)

**Artefacts :**
- Modèle sérialisé (pickle)
- CSV des prévisions de validation
- CSV des prévisions futures

### Intégration MLflow

```
┌─────────────────────────────────────────────────────────┐
│                    MLflow Tracking Server                │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Experiment: nyc_taxi_forecasting                       │
│  ├── Run: prophet_total_trips_20241201_1430             │
│  │   ├── Parameters: model_type=prophet, horizon=30    │
│  │   ├── Metrics: MAE=5234, RMSE=7891, MAPE=12.3%     │
│  │   └── Artifacts: prophet_model.pkl, forecast.csv   │
│  │                                                      │
│  ├── Run: xgboost_total_trips_20241201_1445            │
│  │   ├── Parameters: model_type=xgboost, horizon=30   │
│  │   ├── Metrics: MAE=4987, RMSE=7234, MAPE=11.1%    │
│  │   └── Artifacts: xgboost_model.pkl, forecast.csv  │
│  │                                                      │
│  └── ...                                               │
│                                                          │
│  Model Registry:                                        │
│  └── nyc_taxi_total_trips_prophet                      │
│      ├── Version 1 (Staging)                           │
│      └── Version 2 (Production)                        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## 5. Interface de Visualisation

### Dashboard Django

Le dashboard offre une vue complète des analytics NYC Taxi via une interface web moderne.

**URL :** `http://localhost:8050`

### Pages Disponibles

#### Page d'Accueil (Dashboard)

Vue d'ensemble avec tous les indicateurs clés :

```
┌─────────────────────────────────────────────────────────────────┐
│  🚖 NYC Yellow Cab Analytics                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Total    │ │ Revenue  │ │ Distance │ │ Avg Tip  │           │
│  │ Trips    │ │ Total    │ │ Total    │ │          │           │
│  │ 45.2M    │ │ $892M    │ │ 89.4M mi │ │ $2.34    │           │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │
│                                                                  │
│  ┌─────────────────────────┐ ┌─────────────────────────┐        │
│  │  Monthly Trips Trend    │ │  Monthly Revenue Trend  │        │
│  │  📈                     │ │  📈                     │        │
│  └─────────────────────────┘ └─────────────────────────┘        │
│                                                                  │
│  ┌─────────────────────────┐ ┌─────────────────────────┐        │
│  │  Hourly Heatmap        │ │  Payment Distribution   │        │
│  │  🗓️                     │ │  🥧                     │        │
│  └─────────────────────────┘ └─────────────────────────┘        │
│                                                                  │
│  ┌─────────────────────────────────────────────────────┐        │
│  │  NYC Taxi Zone Activity Map  🗺️                     │        │
│  │  (Choropleth interactif)                            │        │
│  └─────────────────────────────────────────────────────┘        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Page Trends

Analyse détaillée des tendances temporelles :
- Evolution mensuelle trips/revenue
- Comparaison Year-over-Year
- Métriques moyennes dans le temps
- Tendances journalières récentes

#### Page Patterns

Analyse des comportements :
- Heatmap horaire (heure × jour semaine)
- Distribution des distances
- Carte interactive des zones
- Top 20 zones par volume

#### Page Payments

Analyse financière :
- Répartition par mode de paiement
- Parts de marché des vendeurs
- Analyse des pourboires

#### Page Forecasts

Visualisation des modèles ML :
- Graphique prévisions vs historique
- Comparaison des performances modèles
- Historique des runs MLflow
- Métriques d'accuracy

### Technologies Frontend

- **Plotly** : Graphiques interactifs
- **Tailwind CSS** : Styling moderne
- **Responsive** : Adapté mobile/desktop

### Filtrage des Données

Le dashboard filtre automatiquement les données aberrantes :

```python
# Critères de validation
WHERE total_trips > 100              # Évite les jours quasi-vides
  AND total_revenue > 0              # Données cohérentes
  AND avg_trip_revenue BETWEEN 5 AND 200  # Tarifs réalistes
  AND pickup_date <= last_valid_date # Jusqu'au dernier jour complet
```

---

## Installation et Démarrage

### Prérequis

- Docker & Docker Compose
- Compte Google Cloud avec BigQuery activé
- Fichier `gcp-credentials.json`
- **Instance MLflow en fonctionnement** (MLflow n'est pas instancié dans ce projet)

### Démarrage

```bash
# 1. Cloner le repo
git clone <repo_url>
cd airflow

# 2. Configurer les credentials GCP
cp /path/to/credentials.json ./gcp-credentials.json

# 3. Lancer Airflow
docker compose up -d

# 4. Lancer le dashboard Django
cd django_dashboard
pip install -r requirements.txt
python manage.py runserver 0.0.0.0:8050
```

### URLs

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 |
| Django Dashboard | http://localhost:8050 |
| MLflow UI | http://localhost:5555 |

---

## Structure du Projet

```
airflow/
├── dags/
│   ├── nyc_taxi_bronze_ingestion.py    # DAG 1: Ingestion
│   ├── nyc_taxi_silver_dbt.py          # DAG 2: dbt
│   ├── nyc_taxi_gold_aggregations.py   # DAG 3: Agrégations
│   └── nyc_taxi_ml_forecasting.py      # DAG 4: ML
├── dbt/
│   └── nyc_taxi/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       └── dbt_project.yml
├── django_dashboard/
│   ├── dashboard/
│   │   ├── views.py
│   │   ├── charts.py
│   │   ├── bigquery_client.py
│   │   └── mlflow_client.py
│   └── templates/
├── docker-compose.yaml
├── gcp-credentials.json
└── README.md
```

---

## Auteur

Projet de Data Engineering démontrant une architecture moderne de données avec orchestration, transformation, machine learning et visualisation.
