import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import 'medallion-terminal-core/styles'
import './styles.css'
import { Bootstrap } from './app'

const root = document.getElementById('root')
if (!root) throw new Error('Missing #root element')
createRoot(root).render(<StrictMode><Bootstrap /></StrictMode>)
