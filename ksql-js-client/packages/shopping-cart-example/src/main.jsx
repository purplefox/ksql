import React, { useState, useEffect } from 'react';
import { render } from 'react-dom';
import { createKsqlDBConnection } from '@ksqldb/client';
import { Catalog, Basket, Orders, OrderReports } from './components';

const useKsqlDB = (endpoint) => {
  const [state, setState] = useState({
    status: 'connecting',
    db: null,
    error: null,
  });

  useEffect(() => {
    (async () => {
      try {
        const db = await createKsqlDBConnection(endpoint);
        setState({ status: 'connected', db });
      } catch (error) {
        setState({ status: 'error', error });
      }
    })();
  }, [endpoint]);

  return state;
};

const Main = ({ endpoint, userId }) => {
  const { status, db, error } = useKsqlDB(endpoint);

  if(!db) {
    error && console.error(error);

    return <span style={{ border: '1px solid black', padding: 2 }}>
      {status}
    </span>;
  }

  const childProps = {
    db,
    userId,
    style: { flex: 1, padding: '1em', borderLeft: '3px solid #eee' },
  };

  return <div style={{ display: 'flex', height: '100%' }}>
    <Catalog {...childProps} />
    <Basket {...childProps} />
    <Orders {...childProps} style={{ ...childProps.style, maxWidth: '25%' }} />
    <OrderReports {...childProps} />
  </div>;
};

render(
  React.createElement(Main, { endpoint: 'ws://localhost:8888/', userId: 23 }),
  document.getElementsByTagName('main')[0]
);
