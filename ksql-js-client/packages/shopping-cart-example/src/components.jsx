import React, { useState, useEffect } from 'react';

const usePullQuery = (db, query) => {
  const [results, setResults] = useState({ rows: [], error: null });

  useEffect(() => {
    (async () => {
      try {
        const rows = await db.executeQuery(query);
        setResults({ rows });
      } catch (error) {
        setResults({ error });
      }
    })();
  }, [query]);

  return results;
}

const usePushQuery = (db, query) => {
  const [results, setResults] = useState({rows: [], error: null});

  useEffect(() => {
    (async () => {
      const rows = [];
      try {
        const pushResults = await db.streamQuery(query);
        for await (const row of pushResults) {
          rows.push(row);
          setResults({ rows });
        }
      } catch (error) {
        setResults({ error });
      }
    })();
  }, [query]);

  return results;
};

export const Catalog = ({ db, userId, style }) => {
  const { rows, error } = usePullQuery(db, 'SELECT * FROM LINE_ITEM');

  const catalogItems = rows.map(({ item_id, name, price }) => {
    const addToBasket = () => {
      db.insertInto('basket_events', {
        userID: userId,
        itemID: item_id,
        itemName: name,
        quantity: 1,
        amount: 1,
        itemPrice: price,
      });
    };
    return <tr key={item_id}>
      <td>{name}</td>
      <td>{price.toFixed(2)}</td>
      <td><button onClick={addToBasket}>+</button></td>
    </tr>;
  });

  return <div style={style}>
    <h1>Catalog</h1>
    <table style={{cellMargin: 2}}>
      <thead style={{ fontWeight: 600 }}>
        <tr><td>Name</td><td>Price</td></tr>
      </thead>
      <tbody>{catalogItems}</tbody>
    </table>
  </div>;
}

export const Basket = ({ db, userId, style }) => {
  const baseQuery = `SELECT * FROM USER_BASKET WHERE USER_ID = ${userId}`;
  const { rows, error } = usePushQuery(db, `${baseQuery} EMIT CHANGES`);
  const [orderResult, setOrderResult] = useState({status: null, error: null});

  const basketItems = rows.map(({ name, amount }, i) => <li key={i}>{name} ({amount})</li>);
  const total = rows.reduce((acc, { price }) => acc + price, 0);

  const placeOrder = async () => {
    setOrderResult({ status: 'pending' });

    try {
      const basketData = await db.executeQuery(baseQuery, false);

      await db.insertInto('order_event', {
        userID: userId,
        items: basketData.map((x) => x.values),
      });

      setOrderResult({ status: null });
    } catch (error) {
      setOrderResult({ status: 'error', error });
      console.error(error);
    }
  };

  return <div style={style}>
    <h1>Basket</h1>
    <button disabled={!!orderResult.status} onClick={placeOrder}>
      Place order {orderResult.status && `(${orderResult.status})`}
    </button>
    <ul>{basketItems}</ul>
    <div style={{ borderTop: '1px solid black', textAlign: 'right' }}>
      ${total.toFixed(2)}
    </div>
  </div>;
}

export const Orders = ({ db, style }) => {
  const { rows, error } = usePushQuery(db, `SELECT * FROM ORDER_EVENT EMIT CHANGES`);

  const orders = rows.map((x, i) => <pre key={i}>{JSON.stringify(x)}</pre>);

  return <div style={style}>
    <h1>Orders</h1>
    <div style={{ overflowX: 'scroll' }}>
      {orders}
    </div>
  </div>;
}

export const OrderReports = ({ db, style }) => {
  const { rows, error } = usePushQuery(db, `SELECT * FROM ORDER_REPORT EMIT CHANGES`);

  const orderReports = rows.map((x, i) => <pre key={i}>{JSON.stringify(x)}</pre>);

  return <div style={style}>
    <h1>Order reports</h1>
    {orderReports}
  </div>;
}
