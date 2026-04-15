// src/pages/AdminDashboard.tsx
import {
  Box,
  Button,
  TextField,
  Typography,
  Paper,
  Stack,
  Badge,
} from "@mui/material";
import { useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { useApi } from "../contexts/ApiContext";
import { notifySuccess, notifyError } from "../utils/notify";
import { useEffect } from "react";
import OrdersTable from "../components/OrdersTable";

// ✅ Esquema de validación
const schema = yup.object({
  description: yup.string().required("La descripción es obligatoria"),
  amount: yup
    .number()
    .typeError("Debe ser un número")
    .positive("Debe ser mayor que 0")
    .required("El monto es obligatorio"),
});

type FormData = yup.InferType<typeof schema>;

export default function AdminDashboard() {
  const {
    createOrder,
    refreshOrders,
    highlightedOrders,
    clearHighlight,
  } = useApi();

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
    reset,
  } = useForm<FormData>({ resolver: yupResolver(schema) });

  const onSubmit = async (data: FormData) => {
    try {
      await createOrder(data.description, data.amount);
      notifySuccess("Orden creada correctamente 🎉");
      reset();
    } catch (err) {
      console.error(err);
      notifyError("Error al crear la orden");
    }
  };

  // 🔹 Cargar la primera página al montar
  useEffect(() => {
    refreshOrders();
  }, []);

  // 🔹 Cantidad de órdenes nuevas (resaltadas)
  // ⚠️ Cambio: convertimos el Set a array para forzar re-render cuando cambia
  const unreadCount = [...highlightedOrders].length;

  // 🔹 Marcar todas como leídas (borra los resaltados)
  const handleMarkAllAsRead = () => {
    highlightedOrders.forEach((id) => clearHighlight(id));
  };

  return (
    <Paper sx={{ p: 3, mt: 4 }}>
      <Typography variant="h6" gutterBottom>
        Crear nueva orden
      </Typography>

      <Box
        component="form"
        onSubmit={handleSubmit(onSubmit)}
        sx={{ display: "flex", flexDirection: "column", gap: 2 }}
      >
        <TextField
          label="Descripción"
          {...register("description")}
          error={!!errors.description}
          helperText={errors.description?.message}
        />
        <TextField
          label="Monto"
          type="number"
          {...register("amount")}
          error={!!errors.amount}
          helperText={errors.amount?.message}
        />
        <Button
          type="submit"
          variant="contained"
          disabled={isSubmitting}
          sx={{ alignSelf: "center", mt: 2 }}
        >
          {isSubmitting ? "Creando..." : "Crear orden"}
        </Button>
      </Box>

      {/* 📬 Encabezado con badge de nuevas órdenes */}
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ mt: 4 }}
      >
        <Badge
          color="primary"
          badgeContent={unreadCount}
          max={99}
          overlap="rectangular"
        >
          <Typography variant="h6">Órdenes</Typography>
        </Badge>

        <Button
          size="small"
          variant="text"
          onClick={handleMarkAllAsRead}
          disabled={unreadCount === 0}
        >
          Marcar todo como leído
        </Button>
      </Stack>

      {/* 🧾 Tabla de órdenes */}
      <OrdersTable />
    </Paper>
  );
}
