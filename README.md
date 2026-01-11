<div align="center">
  <img src="assets/banner_readme.png" alt="Hybrid DB AI Recommender Banner" width="100%" style="border-radius: 10px;">

  <h1>🧠 Hybrid DB & AI Recommender System</h1>
  <p><strong>Multi-Database Persistence Layer with Machine Learning for E-commerce Insights</strong></p>

<p>
    <a href="https://www.python.org/">
      <img src="https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    </a>
    <a href="https://www.mysql.com/">
      <img src="https://img.shields.io/badge/MySQL-Relational_DB-E4572E?style=for-the-badge&logo=mysql&logoColor=white" alt="MySQL">
    </a>
    <a href="https://www.mongodb.com/">
      <img src="https://img.shields.io/badge/MongoDB-Document_DB-00ED64?style=for-the-badge&logo=mongodb&logoColor=white" alt="MongoDB">
    </a>
    <a href="https://neo4j.com/">
      <img src="https://img.shields.io/badge/Neo4j-Graph_DB-00B2FF?style=for-the-badge&logo=neo4j&logoColor=white" alt="Neo4j">
    </a>
    <a href="https://scikit-learn.org/">
      <img src="https://img.shields.io/badge/AI-Recommender-FFB11B?style=for-the-badge&logo=scikit-learn&logoColor=white" alt="AI Recommender">
    </a>
  </p>
</div>

<hr />

## 📖 Project Overview

**Hybrid-DB-AI-Recommender-System** is an AI-driven data engineering system that combines relational, document, and graph databases to enable scalable analytics and recommendation workflows on Amazon product reviews.

Unlike traditional single-database systems, this project integrates **MySQL** (for structured data), **MongoDB** (for unstructured review logs), and **Neo4j** (for graph relationships) to optimize performance. It features an interactive analytics dashboard and a **Machine Learning Recommender System** based on User-Collaborative Filtering.

---

## 📂 Repository Structure

The project follows a modular structure. Source code is located in `src/`, while datasets are stored in `src/data/`.

```text
├── 📂 assets/                       # Project visuals
│   └── 🖼️ banner_readme.png         # Repository Banner
|
├── 📂 docs/                         # Official Documentation & Reports
│   ├── 📄 Project_Final_Poster.pdf  # Final Project Poster
│   └── 📄 Project_Final_Report.pdf  # Final Project Report
|
├── 📂 src/                          # Source Code
│   ├── 📄 configuracion.py          # Database Credentials & File Paths
│   ├── 📄 load_data.py              # ETL Pipeline (JSON -> MySQL/MongoDB)
│   ├── 📄 inserta_dataset.py        # Incremental Data Loader (Scalability)
│   ├── 📄 menu_visualizacion.py     # Interactive Analytics Dashboard
│   ├── 📄 neo4JProyecto.py          # Graph Modeling & Neo4j Integration
│   |── 📄 machine_learning.py       # AI Recommender System (User Similarity)
│   └── 📂 data/                     # Raw JSON Datasets (Ignored by Git)
│
├── 📄 .gitignore                    # Git configuration
├── 📄 README.md                     # Project Documentation
└── 📄 requirements.txt              # Python dependencies

```

---

## 🛠️ Prerequisites & Installation

To ensure a clean execution environment, using a **Virtual Environment** is highly recommended.

### 1. Environment Setup

```bash
# Create Virtual Environment
python -m venv venv

# Activate Environment

# Windows:
venv\Scripts\activate

# Linux/Mac:
source venv/bin/activate

```

### 2. Install Dependencies

```bash
pip install -r requirements.txt

```

> **Note:** Ensure that you have running instances of **MySQL**, **MongoDB**, and **Neo4j Desktop** and that your credentials are correctly set in `src/configuracion.py`.

---

## 🚀 Execution Pipeline

Since the scripts are located in the `src/` folder, run them using the following commands in order:

### 1️⃣ Data Ingestion (ETL)

**`src/load_data.py`**
Initializes the database ecosystem. It reads the raw JSON files specified in `configuracion.py` and loads them into MySQL and MongoDB.

```bash
python src/load_data.py

```

### 2️⃣ Analytics Dashboard

**`src/menu_visualizacion.py`**
Launches an interactive CLI menu to visualize data insights, such as review evolution, product popularity, and word clouds.

```bash
python src/menu_visualizacion.py

```

### 3️⃣ Graph Modeling

**`src/neo4JProyecto.py`**
Constructs the Graph Database in **Neo4j**, creating nodes and relationships (`User` -> `REVIEWED` -> `Product`) to enable complex queries.

```bash
python src/neo4JProyecto.py

```

### 4️⃣ Incremental Loading (Optional)

**`src/inserta_dataset.py`**
A utility script to inject new datasets into the existing architecture without resetting the system.

```bash
python src/inserta_dataset.py

```

### 5️⃣ AI Recommender System (Optional)

**`src/machine_learning.py`**
Executes the Recommendation Engine. It calculates cosine similarity between users to suggest products that similar users have rated highly.


```bash
python src/machine_learning.py

```

## 👥 Authors

* **Jorge Carnicero Príncipe**
* **Andrés Gil Vicente**

*Completion Date: April 22, 2024*
