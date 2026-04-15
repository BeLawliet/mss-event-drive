// src/contexts/ApiContext.tsx
import React, {
  createContext,
  useContext,
  useEffect,
  useState,
  useCallback,
} from "react";
import { notifyError } from "../utils/notify";

interface Order {
  id: string;
  description: string;
  amount: number;
  status: string;
  created_at: string;
}

interface Notification {
  message: string;
}

interface ApiContextType {
  orders: Order[];
  page: number;
  totalPages: number;
  notifications: Notification[];
  refreshOrders: (page?: number) => Promise<void>;
  createOrder: (description: string, amount: number) => Promise<void>;

  highlightedOrders: Set<string>;
  clearHighlight: (orderId: string) => void;
}

const ApiContext = createContext<ApiContextType>({} as ApiContextType);

export const ApiProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [orders, setOrders] = useState<Order[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [highlightedOrders, setHighlightedOrders] = useState<Set<string>>(
    new Set()
  );

  const API_BASE = "http://127.0.0.1:8000/api/orders";
  const NOTIF_URL = "http://127.0.0.1:8080/api/notifications/stream";

  // 🧩 Helpers de localStorage
  const loadOrderStates = (): {
    highlighted: Set<string>;
    read: Set<string>;
  } => {
    try {
      const raw = localStorage.getItem("orderStates");
      if (!raw) return { highlighted: new Set(), read: new Set() };
      const parsed = JSON.parse(raw);
      return {
        highlighted: new Set(parsed.highlighted || []),
        read: new Set(parsed.read || []),
      };
    } catch {
      return { highlighted: new Set(), read: new Set() };
    }
  };

  const saveOrderStates = (highlighted: Set<string>, read: Set<string>) => {
    localStorage.setItem(
      "orderStates",
      JSON.stringify({
        highlighted: [...highlighted],
        read: [...read],
      })
    );
  };

  // 🧩 Cargar estados al iniciar
  const [, setReadOrders] = useState<Set<string>>(new Set());

  useEffect(() => {
    const { highlighted, read } = loadOrderStates();
    setHighlightedOrders(highlighted);
    setReadOrders(read);
  }, []);

  // 🔹 Cargar órdenes con paginación
  const refreshOrders = useCallback(async (pageNum: number = 1) => {
    try {
      const res = await fetch(`${API_BASE}?page=${pageNum}&page_size=5`, {
        cache: "no-store",
      });
      const data = await res.json();

      const payload = data.data || data;
      const results = payload.results || [];
      const totalPages = payload.total_pages || 1;

      setOrders(results);
      setPage(payload.page || pageNum);
      setTotalPages(totalPages);
    } catch (err) {
      console.error(err);
      notifyError("Error al cargar las órdenes");
    }
  }, []);

  // 🔹 Crear orden
  const createOrder = async (description: string, amount: number) => {
    const res = await fetch(API_BASE, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ description, amount }),
    });

    if (!res.ok) throw new Error("Error al crear orden");
    await refreshOrders(1);
  };

  // 🔹 Suscripción SSE
  useEffect(() => {
    const eventSource = new EventSource(NOTIF_URL);

    eventSource.addEventListener("notification", (event) => {
      try {
        const data = JSON.parse((event as MessageEvent).data);
        setNotifications((prev) => [...prev, data]);

        const match = data.message.match(/order ([\w-]+)/i);
        if (match) {
          const orderId = match[1];
          const { highlighted, read } = loadOrderStates();

          // Solo resaltar si no está leída
          if (!read.has(orderId)) {
            highlighted.add(orderId);
            setHighlightedOrders(new Set(highlighted));
            saveOrderStates(highlighted, read);
          }

          // Actualizar estado local de la orden
          setOrders((prev) =>
            prev.map((o) => (o.id === orderId ? { ...o, status: "PAID" } : o))
          );
        }
      } catch (err) {
        console.error("Error procesando SSE:", err);
      }
    });

    eventSource.onerror = (err) => {
      console.error("Error en EventSource:", err);
      eventSource.close();
    };

    return () => eventSource.close();
  }, [refreshOrders, page]);

  // 🔹 Marcar como leída
  const clearHighlight = (orderId: string) => {
    const { highlighted, read } = loadOrderStates();

    highlighted.delete(orderId);
    read.add(orderId);

    setHighlightedOrders(new Set(highlighted));
    setReadOrders(new Set(read));

    saveOrderStates(highlighted, read);
  };

  return (
    <ApiContext.Provider
      value={{
        orders,
        page,
        totalPages,
        notifications,
        refreshOrders,
        createOrder,
        highlightedOrders,
        clearHighlight,
      }}
    >
      {children}
    </ApiContext.Provider>
  );
};

export const useApi = () => useContext(ApiContext);
