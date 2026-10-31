# FraudShield MA — Étape 1 : Initialisation de l'Arborescence & Découverte des Technologies

Bienvenue dans ce projet ! Même si c'est la toute première fois que vous manipulez ces outils, pas d'inquiétude : chaque étape a un but précis et nous allons avancer brique par brique.

---

## 1. Résumé : Que faisons-nous à cette étape et pourquoi ?

* **Ce qu'on fait :** Nous structurons l'espace de travail dans VS Code en créant les dossiers et les fichiers de configuration de base du projet.
* **Pourquoi on le fait :** Dans un projet Big Data professionnel, il y a plusieurs composants indépendants (génération de données, ingestion streaming, stockage Lakehouse, transformation SQL, Machine Learning). Si tous les fichiers étaient mélangés à la racine, le projet deviendrait vite ingérable. Séparer les responsabilités dès le départ permet à chaque composant d'avoir son rôle bien défini.

---

## 2. Découverte des technologies de l'Étape 1

Voici les premières technologies que vous allez croiser :

| Technologie | C'est quoi ? | À quoi elle sert dans notre projet ? |
| :--- | :--- | :--- |
| **PowerShell** | Le terminal de commande par défaut sous Windows. | Il nous permet de manipuler les fichiers, lancer des scripts et piloter Docker sans passer par l'explorateur graphique. |
| **Git / GitHub** | Un outil de suivi de versions du code (historique de modifications). | Permet de sauvegarder votre travail, revenir en arrière en cas d'erreur et publier votre projet en ligne. |
| **Docker & Docker Compose** | Une solution de conteneurs légers (comme des mini-machines virtuelles isolées). | Permet de lancer Kafka, MinIO et PostgreSQL sur votre PC avec une seule commande sans avoir à les installer un par un sur Windows. |
| **Apache Kafka (ou Redpanda)** | Un bus de messages temps réel à très haut débit. | Il reçoit le flux continu de transactions bancaires envoyées par le générateur pour alimenter Spark. |
| **MinIO (Lakehouse)** | Un serveur de stockage d'objets compatible avec l'API Amazon S3. | Il sert de disque dur pour notre Lakehouse où seront stockées nos tables Delta Lake (Bronze, Silver, Gold). |
| **PostgreSQL** | Une base de données relationnelle SQL robuste. | Sert d'entrepôt de données final (Data Warehouse) pour alimenter vos requêtes analytiques et Power BI. |

---

## 3. Liste des fichiers à créer et leur utilité

Voici les fichiers de base que nous allons créer à la racine du projet :

1. **`.gitignore`** : Indique à Git les dossiers et fichiers temporaires à ne JAMAIS envoyer sur GitHub (mots de passe, environnements virtuels, caches).
2. **`.env.example`** : Un modèle public listant les variables de configuration (identifiants, ports, mots de passe fictifs).
3. **`.env`** : Le fichier réel utilisé par Docker et vos scripts pour lire les mots de passe et configurations (ce fichier reste secret sur votre PC).
4. **`README.md`** : Le document de présentation principal du projet visible sur la page d'accueil de votre dépôt GitHub.
5. **`requirements.txt`** : La liste des bibliothèques Python nécessaires pour exécuter les scripts du projet.
6. **`docker-compose.yml`** : La recette qui décrit et orchestre tous les conteneurs (Kafka/Redpanda, MinIO, PostgreSQL).

---

## 4. Guide pas à pas : Création manuelle des fichiers dans PowerShell

Ouvrez le terminal PowerShell dans VS Code (au niveau du dossier racine `fraudshield-ma`) et créez les fichiers un par un avec la commande `New-Item` :

### Étape 4.1 : Créer le fichier `.gitignore`
```powershell
New-Item -ItemType File -Name ".gitignore"
```

### Étape 4.2 : Créer le modèle d'environnement `.env.example`
```powershell
New-Item -ItemType File -Name ".env.example"
```

### Étape 4.3 : Créer le fichier d'environnement réel `.env`
```powershell
New-Item -ItemType File -Name ".env"
```

### Étape 4.4 : Créer la documentation principale `README.md`
```powershell
New-Item -ItemType File -Name "README.md"
```

### Étape 4.5 : Créer la liste des dépendances Python `requirements.txt`
```powershell
New-Item -ItemType File -Name "requirements.txt"
```

### Étape 4.6 : Créer le fichier de configuration de l'infrastructure `docker-compose.yml`
```powershell
New-Item -ItemType File -Name "docker-compose.yml"
```

---

## 5. Résultat attendu dans VS Code

Votre arborescence complète doit maintenant ressembler exactement à ceci :

```text
fraudshield-ma/
├── .env
├── .env.example
├── .gitignore
├── README.md
├── docker-compose.yml
├── requirements.txt
├── airflow/
│   └── dags/
├── data_generator/
├── dbt_fraudshield/
├── ml/
│   ├── models/
│   └── notebooks/
├── spark/
│   ├── jobs/
│   └── utils/
└── tests/
```



Étape 3 : Mise en place de l'infrastructure de base (Docker Compose)
Ce que nous faisons et pourquoi
Nous allons configurer le fichier docker-compose.yml. Ce fichier agit comme une recette qui télécharge et démarre simultanément tous les serveurs et bases de données dont votre projet a besoin en une seule commande, sans avoir à les installer manuellement sur votre ordinateur. 
Cela garantit que le projet fonctionnera exactement de la même manière sur n'importe quelle machine.  
 Définitions des technologies utilisées pour le MVP  
  Docker Compose : Un outil qui permet de faire tourner plusieurs applications isolées (appelées conteneurs) en même temps à partir d'un seul fichier de configuration.
  Redpanda (Alternative à Kafka) : C'est le système de messagerie en temps réel du projet. Il va recevoir le flux de transactions généré par votre script Python.
   Nous utilisons Redpanda car il est totalement compatible avec Kafka mais consomme beaucoup moins de mémoire (RAM), ce qui est idéal pour un environnement local. 
     MinIO : C'est un système de stockage de fichiers (similaire au cloud AWS S3). 
     Il servira à stocker vos données sous forme de Data Lake (couches Bronze, Silver et Gold).   PostgreSQL : Une base de données relationnelle classique. 
     Elle servira d'entrepôt de données final pour construire le modèle en étoile et se connecter au tableau de bord Power BI.


Étape 5 : Préparation et création du générateur de transactions Python
Ce que nous faisons et pourquoi
Dans un projet réel de détection de fraude, les données proviennent des systèmes internes de la banque. N'ayant pas accès à de vraies données bancaires (pour des raisons évidentes de confidentialité), nous devons créer un programme qui va simuler un flux continu de transactions. Ce script est le point de départ de tout le projet : il crée de fausses transactions (avec des dirhams, des villes marocaines, des identifiants) et les envoie en direct dans notre système de messagerie Redpanda (Kafka). 
  Définitions des technologies utilisées 
  Python : Le langage de programmation principal pour ce script.Faker : Une bibliothèque Python très populaire qui permet de générer des données aléatoires mais réalistes (noms, adresses, dates, etc.).  
   confluent-kafka : Le connecteur qui permet à notre script Python de discuter avec notre serveur Redpanda/Kafka pour y envoyer les messages.
   JSON : Le format de texte utilisé pour structurer chaque transaction (ressemble à un dictionnaire avec des clés et des valeurs).


Étape 6 : Exécution du générateur et validation du flux de données

Ce que nous faisons et pourquoi
Nous allons exécuter le script Python pour lancer la simulation. Le programme va créer un flux continu de fausses transactions bancaires et les envoyer dans le "topic" (le canal) nommé "transactions" de notre serveur Redpanda. La réussite de cette étape marque la validation de votre MVP (Produit Minimum Viable) de 2 heures exigé par le cahier des charges.

ش نفهموها بالدارجة مزيان، الكود كيدير بحال هكا باش يحدد شحال ديال الفلوس (amount) غتكون فهاد العملية:

يلا كانت العملية فيها نصب (Fraude): الكود كيتعامل بحال يلا شي شفار أو هاكر سرق الكارطة، وكيعزل واحد المبلغ عشوائي وكبير بزاف ما بين 5000 و 50.000 درهم.

يلا كانت عملية عادية (مفيهاش نصب): الكود كيعطينا مبلغ عادي ومقبول ديال التقضية اليومية كيتراوح ما بين 10 دراهم و 2000 درهم.

الدالة round(..., 2): الدور ديالها هو تقطع الحساب باش يبقاو غير جوج أرقام مورا الفاصلة (مثلا 120.50 درهم بلاصة 120.50438 درهم)، باش داكشي يبان حقيقي ومنطقي بحال الفلوس د بصح.


Étape 7 : Création de la couche Bronze avec Apache SparkCe que nous faisons et pourquoi
Maintenant que le générateur envoie les transactions, l'objectif de la Semaine 2 est de construire la couche Bronze et Silver. Nous allons d'abord créer un job Apache Spark (Structured Streaming) qui écoute le canal Kafka en continu et enregistre chaque transaction brute, sans modification, dans le stockage MinIO. Cette couche sert d'archive sécurisée et utilise un "checkpoint" obligatoire pour garantir qu'aucune donnée ne soit perdue ou dupliquée si le système redémarre.   Définitions des technologies utiliséesApache Spark (PySpark) : Un moteur de calcul conçu pour traiter massivement les données. Son module "Structured Streaming" permet de capturer et de traiter le flux de transactions en temps réel.   Delta Lake : Un format de stockage spécialisé qui se place au-dessus de MinIO. Il apporte la fiabilité aux Data Lakes en évitant la corruption des fichiers lors des écritures en continu.


Étape 8 : Installation de PySpark et écriture du script BronzeCe que nous faisons et pourquoi
Nous installons PySpark sur votre machine et rédigeons le code qui va capturer les messages de Redpanda (Kafka) pour les sauvegarder dans MinIO. C'est la couche "Bronze" : elle stocke la donnée brute exactement telle qu'elle a été générée, sans aucune altération, en incluant ses métadonnées Kafka comme l'offset ou le timestamp.   Définitions des connecteurs (JARs)
Spark a besoin de "traduire" la communication avec d'autres outils. Pour cela, on configure le téléchargement dynamique de paquets Java (JARs) :spark-sql-kafka : Permet à Spark de lire les messages venant du topic Kafka/Redpanda.   hadoop-aws : Permet à Spark de parler avec MinIO (qui utilise le même protocole d'échange qu'Amazon AWS S3).delta-spark : Ajoute le format Delta Lake pour sécuriser l'écriture des fichiers et créer notre Lakehouse



Étape 9 : Exécution de l'ingestion Bronze et vérification dans MinIO

Ce que nous faisons et pourquoi
Nous allons faire communiquer les deux premiers composants de notre architecture. Le générateur Python va envoyer les transactions dans Redpanda/Kafka, et en même temps, le job Apache Spark va les lire en continu pour les écrire dans le bucket bronze de MinIO au format Delta. Cela valide l'exigence F2 de votre cahier des charges.



3. Vérification visuelle dans le Data Lake
Pendant que les deux terminaux tournent en parallèle, retournez sur l'interface web de MinIO (http://localhost:9001) et ouvrez votre bucket bronze. Vous devriez y voir apparaître deux dossiers :

transactions/ : Contient les données brutes (fichiers .parquet et le journal de transactions _delta_log/).

checkpoints/ : Contient les fichiers de suivi de Spark pour garantir la tolérance aux pannes et l'absence de doublons.



Étape 10 : La couche Silver (Nettoyage et Features)

Si vous voyez bien vos données dans MinIO, dites-le-moi ! La prochaine étape consistera à lire ces données "Bronze", à les nettoyer (typage, suppression des doublons), et à calculer de nouvelles variables utiles pour l'Intelligence Artificielle (comme le nombre de transactions de la carte sur les 10 dernières minutes), pour enfin les sauvegarder dans la couche Silver



Étape 11 : Enrichissement des données (Features glissantes pour l'IA)

Ce que nous faisons et pourquoi
Pour qu'une Intelligence Artificielle puisse détecter efficacement une fraude, elle a besoin de contexte comportemental. Nous allons enrichir notre flux de données en calculant des "Features glissantes" (variables historiques) exigées par votre cahier des charges. L'objectif est de créer de nouvelles colonnes telles que le nombre de transactions effectuées par la même carte dans les 10 dernières minutes (pour repérer les attaques en rafale) ou l'heure de la journée.   
DOCX
+ 1

Définitions des concepts utilisés

Feature Engineering (Création de variables) : L'action de combiner ou transformer des données brutes pour créer des indicateurs mathématiques qui aident le modèle prédictif à comprendre la situation.

Fonctions de fenêtrage (Window) : Des opérations Spark qui permettent d'isoler un groupe spécifique (ex: toutes les transactions du client_id "C-1234") et de calculer une statistique sur une période donnée (ex: l'écart du montant actuel par rapport à sa moyenne des 30 derniers jours).   
DOCX

foreachBatch : Une technique de Spark Streaming mentionnée dans les parades aux risques de votre document. Elle permet de mettre le flux en pause une fraction de seconde, de comparer les nouvelles transactions avec l'historique déjà stocké dans Silver, d'appliquer les calculs de fenêtrage, puis de sauvegarder le résultat.   
DOCX

Dites-moi quand vous êtes prêt à coder ce script d'enrichissement. C'est l'une des parties les plus techniques et intéressantes du projet !




Étape 11 : Calcul des Features glissantes avec Spark (foreachBatch)Pour doter notre Intelligence Artificielle d'un véritable contexte, nous allons créer un nouveau script qui lit les données nettoyées, calcule les indicateurs comportementaux, et les sauvegarde. 
Nous utiliserons la méthode foreachBatch pour analyser chaque lot de nouvelles transactions.   
Définitions des concepts utilisés
Cast (Typage de date) : Pour calculer du temps (comme les 10 dernières minutes), Spark a besoin que la colonne texte event_time soit convertie en un vrai format horodaté (TimestampType).

Partitionnement (Window) : Nous allons regrouper virtuellement les données par card_id (la carte bancaire) et les trier par ordre chronologique pour compter combien de fois cette carte a été utilisée récemment.1. Préparation dans MinIO
Sur l'interface web de MinIO (http://localhost:9001), assurez-vous que le bucket silver est prêt. Nous allons y créer automatiquement un nouveau dossier transactions_features.


Étape 12 : Introduction au Machine Learning et configuration de MLflowCe que nous faisons et pourquoi
Vos données sont maintenant nettoyées et enrichies dans la couche Silver. L'objectif de cette étape est d'entraîner notre Intelligence Artificielle (le modèle prédictif) pour qu'elle apprenne à différencier une transaction normale d'une fraude, comme exigé dans le cahier des charges. Nous utiliserons MLflow pour enregistrer ce modèle et suivre ses performances (Précision, Rappel, F1). 

 Définitions des technologies utilisées

 scikit-learn et XGBoost : Les bibliothèques de Machine Learning requises pour ce projet. Elles contiennent les algorithmes (Isolation Forest pour le non supervisé, XGBoost pour le supervisé) qui vont analyser les comportements suspects (montant, heure, fréquence). 

   MLflow : Un outil de suivi des expérimentations. Il agit comme un carnet de bord automatique qui sauvegarde votre modèle final et trace ses métriques pour pouvoir le réutiliser plus tard lors du scoring en temps réel.
   
      Pandas et deltalake : Des outils qui permettent de lire les fichiers Delta stockés dans MinIO et de les transformer en un tableau classique, le format obligatoire pour entraîner les algorithmes scikit-learn.



      Étape 13 : Le Scoring en temps réel (Couche Gold)

Dites-moi quand vous avez exploré l'interface MLflow et que vous êtes prêt à continuer. La prochaine étape consistera à créer un nouveau job Spark Streaming qui va charger ce modèle "Production", écouter le flux de transactions en direct, attribuer un score de risque à chacune, et déclencher une alerte dans la couche Gold si le score est trop élevé


Objectif de l'Étape 13
Passer de la théorie à la pratique en production. Jusqu'à présent, nous avons entraîné un modèle statique. Cette étape consiste à créer un script de Streaming en direct qui fait "vivre" ce modèle.

Comment ça va se passer en pratique ?
Chargement du modèle MLflow : Le script va se connecter au modèle que nous venons d'enregistrer (FraudShield_Production).

Écoute du flux (Silver vers Gold) : Le job Spark Streaming intercepte en continu les nouvelles transactions nettoyées et enrichies.

Inférence en temps réel (Scoring) : Pour chaque transaction qui arrive, le modèle calcule instantanément une probabilité de fraude (par exemple, un score de 0.02 pour une opération normale, ou 0.95 pour un piratage).

Filtrage et Alerte (Couche Gold) :

Les transactions normales sont archivées.

Les transactions suspectes (au-delà d'un seuil de risque) sont immédiatement isolées et écrites dans un nouveau bucket MinIO nommé gold. C'est cette couche "Gold" qui servira de base pour déclencher les alertes de sécurité.

Les technologies clés utilisées :
Spark UDF (User Defined Function) ou Spark MLlib : Permet d'injecter et d'exécuter notre modèle XGBoost directement à l'intérieur du moteur de calcul distribué.

MinIO (Bucket gold) : Le stockage final hautement qualitatif, prêt à être consommé par l'interface utilisateur.