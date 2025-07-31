{{/*
Expand the name of the chart.
*/}}
{{- define "kafka-ops-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kafka-ops-agent.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kafka-ops-agent.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafka-ops-agent.labels" -}}
helm.sh/chart: {{ include "kafka-ops-agent.chart" . }}
{{ include "kafka-ops-agent.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafka-ops-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka-ops-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: api
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kafka-ops-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafka-ops-agent.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the database URL
*/}}
{{- define "kafka-ops-agent.databaseUrl" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "postgresql://%s:${DATABASE_PASSWORD}@%s-postgresql:%d/%s" .Values.postgresql.auth.username (include "kafka-ops-agent.fullname" .) (.Values.postgresql.primary.service.ports.postgresql | int) .Values.postgresql.auth.database }}
{{- else }}
{{- printf "postgresql://%s:${DATABASE_PASSWORD}@%s:%d/%s" .Values.externalServices.database.username .Values.externalServices.database.host (.Values.externalServices.database.port | int) .Values.externalServices.database.name }}
{{- end }}
{{- end }}

{{/*
Create Kafka bootstrap servers
*/}}
{{- define "kafka-ops-agent.kafkaBootstrapServers" -}}
{{- .Values.externalServices.kafka.bootstrapServers }}
{{- end }}

{{/*
Create Redis URL
*/}}
{{- define "kafka-ops-agent.redisUrl" -}}
{{- if .Values.redis.enabled }}
{{- printf "redis://:%s@%s-redis-master:6379/0" .Values.redis.auth.password (include "kafka-ops-agent.fullname" .) }}
{{- else }}
{{- printf "redis://localhost:6379/0" }}
{{- end }}
{{- end }}

{{/*
Create image pull secrets
*/}}
{{- define "kafka-ops-agent.imagePullSecrets" -}}
{{- if or .Values.image.pullSecrets .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- range .Values.image.pullSecrets }}
  - name: {{ . }}
{{- end }}
{{- range .Values.global.imagePullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create storage class
*/}}
{{- define "kafka-ops-agent.storageClass" -}}
{{- if .Values.persistence.storageClass }}
{{- .Values.persistence.storageClass }}
{{- else if .Values.global.storageClass }}
{{- .Values.global.storageClass }}
{{- end }}
{{- end }}