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

  const childProps = { db, userId };
  const childStyle = { flex: 1, padding: '1em', borderLeft: '3px solid #eee' };

  return <div style={{ display: 'flex', height: '100%' }}>
    <div style={childStyle}>
      <Catalog {...childProps} />
    </div>
    <div style={childStyle}>
      <Basket {...childProps} />
    </div>
    <div style={{ ...childStyle, maxWidth: '25%' }}>
      <Orders {...childProps} />
    </div>
    <div style={childStyle}>
      <OrderReports {...childProps} />
    </div>
  </div>;
};

render(
  React.createElement(Main, { endpoint: 'ws://localhost:8888/', userId: 23 }),
  document.getElementsByTagName('main')[0]
);
