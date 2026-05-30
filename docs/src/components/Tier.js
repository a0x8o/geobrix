import React from 'react';
import styles from './Tier.module.css';

// Availability badge for Execution Tiers. Usage in MDX:
//   <Tier both/>  <Tier light/>  <Tier heavy/>
export default function Tier({both, light, heavy}) {
  const pills = [];
  if (both || light) pills.push(['light', 'Lightweight']);
  if (both || heavy) pills.push(['heavy', 'Heavyweight']);
  return (
    <span className={styles.tierGroup}>
      {pills.map(([k, label]) => (
        <span key={k} className={`${styles.tier} ${styles[k]}`}>{label}</span>
      ))}
    </span>
  );
}
