# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copiar el código del backend
COPY backend-go/ .

# Descargar dependencias y compilar
RUN go mod download
RUN go build -o main .

# Run stage
FROM alpine:latest

WORKDIR /app

# Copiar el binario compilado
COPY --from=builder /app/main .

# Exponer el puerto
EXPOSE 8080

# Comando para ejecutar
CMD ["./main"]