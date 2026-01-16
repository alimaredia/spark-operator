## List of Manifests

### Service Accounts

```
oc get serviceaccounts -n spark-operator-openshift
```

```yaml
---
kind: ServiceAccount
metadata:
  name: spark-operator-controller
  namespace: spark-operator
---
kind: ServiceAccount
metadata:
  name: spark-operator-spark --- This DNE
  namespace: default
---
kind: ServiceAccount
metadata:
  name: spark-operator-webhook
  namespace: spark-operator
```

### ClusterRoles

```
oc get clusterroles -n spark-operator-openshift | grep "spark"
```
```yaml
---
kind: ClusterRole
metadata:
  name: spark-operator-controller
  namespace: spark-operator
---
kind: ClusterRole
metadata:
  name: spark-operator-webhook
  namespace: spark-operator
```

### ClusterRoleBindings

```
oc get clusterrolebindings -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: ClusterRoleBinding
metadata:
  name: spark-operator-controller
  namespace: spark-operator
---
kind: ClusterRoleBinding
metadata:
  name: spark-operator-webhook
  namespace: spark-operator
```

### Roles

```
oc get roles -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: Role
metadata:
  name: spark-operator-controller
  namespace: spark-operator
---
kind: Role
metadata:
  name: spark-operator-controller -- This DNE
  namespace: default
---
kind: Role
metadata:
  name: spark-operator-spark -- This DNE
  namespace: default
---
kind: Role
metadata:
  name: spark-operator-webhook
  namespace: spark-operator
---
kind: Role
metadata:
  name: spark-operator-webhook -- This DNE
  namespace: default
```

### Rolebindings

```
oc get rolebindings -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: RoleBinding
metadata:
  name: spark-operator-controller
  namespace: spark-operator
---
kind: RoleBinding
metadata:
  name: spark-operator-controller -- This DNE
  namespace: default
---
kind: RoleBinding
metadata:
  name: spark-operator-spark -- This DNE
  namespace: default
---
kind: RoleBinding
metadata:
  name: spark-operator-webhook
  namespace: spark-operator
---
kind: RoleBinding
metadata:
  name: spark-operator-webhook -- This DNE
  namespace: default
```

### Services

```
oc get services -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: Service
metadata:
  name: spark-operator-webhook-svc
  labels:
```


### Deployments

```
oc get deployments -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: Deployment
metadata:
  name: spark-operator-controller
  labels:
---
kind: Deployment
metadata:
  name: spark-operator-webhook
  labels:
```

### Mutating Webhook Configuration

```
oc get mutatingwebhookconfiguration -n spark-operator-openshift | grep "spark"
```

```yaml
---
kind: MutatingWebhookConfiguration
metadata:
  name: spark-operator-webhook
  labels:
---
```

### Validating Webhook Configuration

```
oc get validatingwebookconfiguration -n spark-operator-openshift | grep "spark"
```

```yaml
kind: ValidatingWebhookConfiguration
metadata:
  name: spark-operator-webhook
```

### Custom Reource Definitions

```
oc get customresourcedefinition -n spark-operator-openshift | grep "spark"
...
scheduledsparkapplications.sparkoperator.k8s.io
sparkapplications.sparkoperator.k8s.io
sparkconnects.sparkoperator.k8s.io
```
