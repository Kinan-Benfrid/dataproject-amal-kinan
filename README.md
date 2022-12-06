# dataproject-amal-kinan

## Projet a pour but de manipuler des donnees dans HDFS à partir d'un CSV : supprimer ou hasher des donnees client à partir d'id 

### Pour avoir de l'aide sur comment lancer les services

```
-help
```
![image](https://user-images.githubusercontent.com/77750495/206045257-4874e8a4-e834-43ac-9b3c-de7755bc886e.png)

### Pour demander la suppression des donnees d'un client à partir d'un id

```
-delete <id_du_client>
```

### Pour demander le hashage des donnees d'un client à partir d'un id

```
-hash <id_du_client>
```


### Pour demander le hashage des donnees d'un client à partir d'un id et la suppression des donnees d'un client à partir d'un id

```
-delete <id_du_client_a_delete> -hash <id_du_client_a_hasher>
```

### Avant hashage

![image](https://user-images.githubusercontent.com/77750495/206041645-2bdd300c-9a25-4cf9-9119-f2b7790ba4f5.png)


### Apres le hashage

![image](https://user-images.githubusercontent.com/77750495/206041877-6e0cea3f-e08e-465a-b1de-3d21a9adc4d4.png)


### Avant delete

![image](https://user-images.githubusercontent.com/77750495/206042567-ddf39bc9-8adf-4a33-a30c-4de3daa742ee.png)


### Apres delete

![image](https://user-images.githubusercontent.com/77750495/206042499-e8237b83-07b2-44a1-9717-3685bacc2a9f.png)
