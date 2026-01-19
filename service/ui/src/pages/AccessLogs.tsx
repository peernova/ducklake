import { useEffect, useState, useRef } from 'react';
import {
  Activity,
  Users,
  Clock,
  Database,
  TrendingUp,
  RefreshCw,
  Filter,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  Search,
  X,
  Calendar,
  Eye,
} from 'lucide-react';

// Types (defined early for use in components)
interface RecentEvent {
  timestamp: string;
  event_id: string;
  user_id: string;
  user_email: string;
  resource_type: string;
  resource_id: string;
  operation: string;
  status: string;
  trace_id: string;
  log_attributes?: Record<string, string>;
}

// Mini Calendar Component
interface MiniCalendarProps {
  month: Date;
  selectedStart: Date | null;
  selectedEnd: Date | null;
  onSelectDate: (date: Date) => void;
  onPrevMonth: () => void;
  onNextMonth: () => void;
}

function MiniCalendar({ month, selectedStart, selectedEnd, onSelectDate, onPrevMonth, onNextMonth }: MiniCalendarProps) {
  const daysInMonth = new Date(month.getFullYear(), month.getMonth() + 1, 0).getDate();
  const firstDay = new Date(month.getFullYear(), month.getMonth(), 1).getDay();
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const days = [];
  for (let i = 0; i < firstDay; i++) {
    days.push(null);
  }
  for (let i = 1; i <= daysInMonth; i++) {
    days.push(new Date(month.getFullYear(), month.getMonth(), i));
  }

  const isSelected = (date: Date | null) => {
    if (!date) return false;
    if (selectedStart && date.getTime() === selectedStart.getTime()) return true;
    if (selectedEnd && date.getTime() === selectedEnd.getTime()) return true;
    return false;
  };

  const isInRange = (date: Date | null) => {
    if (!date || !selectedStart || !selectedEnd) return false;
    return date > selectedStart && date < selectedEnd;
  };

  const isToday = (date: Date | null) => {
    if (!date) return false;
    return date.getTime() === today.getTime();
  };

  const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  return (
    <div style={{ width: '220px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
        <button
          type="button"
          onClick={onPrevMonth}
          style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '4px', color: '#64748b', borderRadius: '4px' }}
          onMouseEnter={(e) => e.currentTarget.style.background = '#f1f5f9'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'none'}
        >
          <ChevronLeft size={16} />
        </button>
        <span style={{ fontSize: '13px', fontWeight: 600, color: '#1e293b' }}>
          {monthNames[month.getMonth()]} {month.getFullYear()}
        </span>
        <button
          type="button"
          onClick={onNextMonth}
          style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '4px', color: '#64748b', borderRadius: '4px' }}
          onMouseEnter={(e) => e.currentTarget.style.background = '#f1f5f9'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'none'}
        >
          <ChevronRight size={16} />
        </button>
      </div>

      {/* Day headers */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: '2px', marginBottom: '4px' }}>
        {['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'].map(d => (
          <div key={d} style={{ fontSize: '10px', fontWeight: 600, color: '#94a3b8', textAlign: 'center', padding: '4px' }}>
            {d}
          </div>
        ))}
      </div>

      {/* Days grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: '2px' }}>
        {days.map((date, i) => (
          <div
            key={i}
            onClick={() => date && onSelectDate(date)}
            style={{
              padding: '6px 4px',
              fontSize: '12px',
              textAlign: 'center',
              cursor: date ? 'pointer' : 'default',
              borderRadius: '4px',
              background: isSelected(date) ? 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)' : isInRange(date) ? '#dbeafe' : 'transparent',
              color: isSelected(date) ? 'white' : isToday(date) ? '#3b82f6' : date ? '#1e293b' : 'transparent',
              fontWeight: isSelected(date) || isToday(date) ? 600 : 400,
              transition: 'all 0.1s ease',
            }}
            onMouseEnter={(e) => {
              if (date && !isSelected(date)) {
                e.currentTarget.style.background = '#f1f5f9';
              }
            }}
            onMouseLeave={(e) => {
              if (date && !isSelected(date) && !isInRange(date)) {
                e.currentTarget.style.background = 'transparent';
              } else if (isInRange(date)) {
                e.currentTarget.style.background = '#dbeafe';
              }
            }}
          >
            {date?.getDate() || ''}
          </div>
        ))}
      </div>
    </div>
  );
}

// JSON Tree Viewer Component
interface JsonTreeProps {
  data: unknown;
  name?: string;
  level?: number;
  defaultExpanded?: boolean;
}

function JsonTree({ data, name, level = 0, defaultExpanded = true }: JsonTreeProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  const indent = level * 16;
  const isObject = data !== null && typeof data === 'object';
  const isArray = Array.isArray(data);

  if (!isObject) {
    // Primitive value
    const color = typeof data === 'string' ? '#22c55e'
      : typeof data === 'number' ? '#3b82f6'
      : typeof data === 'boolean' ? '#f59e0b'
      : '#94a3b8';
    return (
      <div style={{ paddingLeft: indent, display: 'flex', gap: '8px', padding: '2px 0 2px ' + indent + 'px' }}>
        {name && <span style={{ color: '#e879f9' }}>"{name}"</span>}
        {name && <span style={{ color: '#94a3b8' }}>:</span>}
        <span style={{ color }}>
          {typeof data === 'string' ? `"${data}"` : String(data)}
        </span>
      </div>
    );
  }

  const entries = isArray ? data.map((v, i) => [i, v] as [number, unknown]) : Object.entries(data as Record<string, unknown>);
  const isEmpty = entries.length === 0;
  const bracket = isArray ? ['[', ']'] : ['{', '}'];

  return (
    <div style={{ fontFamily: 'Monaco, Consolas, monospace', fontSize: '12px' }}>
      <div
        style={{
          paddingLeft: indent,
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
          cursor: isEmpty ? 'default' : 'pointer',
          padding: '2px 0 2px ' + indent + 'px',
        }}
        onClick={() => !isEmpty && setExpanded(!expanded)}
      >
        {!isEmpty && (
          <ChevronRight
            size={12}
            style={{
              transform: expanded ? 'rotate(90deg)' : 'none',
              transition: 'transform 0.15s ease',
              color: '#64748b',
            }}
          />
        )}
        {name !== undefined && (
          <>
            <span style={{ color: '#e879f9' }}>"{name}"</span>
            <span style={{ color: '#94a3b8' }}>:</span>
          </>
        )}
        <span style={{ color: '#94a3b8' }}>
          {bracket[0]}
          {!expanded && !isEmpty && <span style={{ color: '#64748b' }}> ... {entries.length} items </span>}
          {(!expanded || isEmpty) && bracket[1]}
        </span>
      </div>
      {expanded && !isEmpty && (
        <>
          {entries.map(([key, value], idx) => (
            <JsonTree
              key={String(key)}
              data={value}
              name={isArray ? undefined : String(key)}
              level={level + 1}
              defaultExpanded={level < 1}
            />
          ))}
          <div style={{ paddingLeft: indent, color: '#94a3b8', padding: '2px 0 2px ' + indent + 'px' }}>
            {bracket[1]}
          </div>
        </>
      )}
    </div>
  );
}

// Log Detail Modal Component
interface LogDetailModalProps {
  event: RecentEvent;
  onClose: () => void;
  onFilterByTrace?: (traceId: string) => void;
}

function LogDetailModal({ event, onClose, onFilterByTrace }: LogDetailModalProps) {
  const [jsonExpanded, setJsonExpanded] = useState(false);

  const fields = [
    { label: 'Timestamp', value: new Date(event.timestamp).toLocaleString(), icon: '🕐' },
    { label: 'User ID', value: event.user_id, icon: '👤' },
    { label: 'User Email', value: event.user_email, icon: '📧' },
    { label: 'Resource Type', value: event.resource_type, icon: '📦' },
    { label: 'Resource ID', value: event.resource_id, icon: '🔗' },
    { label: 'Operation', value: event.operation, icon: '⚡' },
    { label: 'Status', value: event.status, icon: event.status === 'success' ? '✅' : '❌' },
    { label: 'Event ID', value: event.event_id, icon: '🆔', mono: true },
    { label: 'Trace ID', value: event.trace_id, icon: '🔍', mono: true },
  ];

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'rgba(0,0,0,0.6)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
        backdropFilter: 'blur(4px)',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: 'white',
          borderRadius: '16px',
          maxWidth: '720px',
          width: '95%',
          maxHeight: '85vh',
          overflow: 'hidden',
          boxShadow: '0 25px 80px rgba(0,0,0,0.35)',
          display: 'flex',
          flexDirection: 'column',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '20px 24px',
          borderBottom: '1px solid #e2e8f0',
          background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)',
        }}>
          <div>
            <h3 style={{ margin: 0, fontSize: '18px', fontWeight: 600, color: '#1e293b' }}>Event Details</h3>
            <span style={{ fontSize: '12px', color: '#64748b' }}>{event.event_id}</span>
          </div>
          <button
            onClick={onClose}
            style={{
              background: '#f1f5f9',
              border: 'none',
              cursor: 'pointer',
              padding: '8px',
              color: '#64748b',
              borderRadius: '8px',
              display: 'flex',
              alignItems: 'center',
              transition: 'all 0.15s ease',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.background = '#fee2e2'; e.currentTarget.style.color = '#ef4444'; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = '#f1f5f9'; e.currentTarget.style.color = '#64748b'; }}
          >
            <X size={18} />
          </button>
        </div>

        {/* Content */}
        <div style={{ padding: '24px', overflow: 'auto', flex: 1 }}>
          {/* Status Badge */}
          <div style={{ marginBottom: '20px', display: 'flex', gap: '12px', alignItems: 'center' }}>
            <span style={{
              padding: '6px 14px',
              borderRadius: '20px',
              fontSize: '13px',
              fontWeight: 600,
              background: event.status === 'success' ? '#dcfce7' : '#fee2e2',
              color: event.status === 'success' ? '#166534' : '#dc2626',
            }}>
              {event.status?.toUpperCase()}
            </span>
            <span style={{
              padding: '6px 14px',
              borderRadius: '20px',
              fontSize: '13px',
              fontWeight: 500,
              background: '#e0e7ff',
              color: '#4338ca',
            }}>
              {event.operation}
            </span>
            <span style={{
              padding: '6px 14px',
              borderRadius: '20px',
              fontSize: '13px',
              fontWeight: 500,
              background: '#f1f5f9',
              color: '#475569',
            }}>
              {event.resource_type}
            </span>
          </div>

          {/* Fields Grid */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '16px', marginBottom: '20px' }}>
            {fields.filter(f => f.value).map(({ label, value, mono }) => (
              <div key={label} style={{
                padding: '14px 16px',
                background: '#f8fafc',
                borderRadius: '10px',
                border: '1px solid #e2e8f0',
              }}>
                <div style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', fontWeight: 600, marginBottom: '6px', letterSpacing: '0.5px' }}>
                  {label}
                </div>
                <div style={{
                  fontSize: '13px',
                  color: '#1e293b',
                  fontFamily: mono ? 'monospace' : 'inherit',
                  wordBreak: 'break-all',
                  fontWeight: 500,
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                }}>
                  {value}
                  {label === 'Trace ID' && value && (
                    <>
                      <a
                        href={`http://localhost:16686/trace/${value}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          fontSize: '11px',
                          color: '#3b82f6',
                          textDecoration: 'none',
                          padding: '2px 8px',
                          background: '#eff6ff',
                          borderRadius: '4px',
                          fontFamily: 'inherit',
                        }}
                      >
                        Traces ↗
                      </a>
                      {onFilterByTrace && (
                        <button
                          onClick={() => { onFilterByTrace(value); onClose(); }}
                          style={{
                            fontSize: '11px',
                            color: '#7c3aed',
                            background: '#f3e8ff',
                            border: 'none',
                            padding: '2px 8px',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontFamily: 'inherit',
                          }}
                        >
                          Filter logs
                        </button>
                      )}
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>

          {/* Log Attributes */}
          {event.log_attributes && Object.keys(event.log_attributes).length > 0 && (
            <div style={{ border: '1px solid #e2e8f0', borderRadius: '10px', overflow: 'hidden', marginBottom: '16px' }}>
              <div style={{
                padding: '14px 16px',
                background: '#f8fafc',
                borderBottom: '1px solid #e2e8f0',
                fontSize: '13px',
                fontWeight: 600,
                color: '#475569',
              }}>
                Log Attributes
              </div>
              <div style={{
                background: '#1e293b',
                color: '#e2e8f0',
                padding: '16px',
                overflow: 'auto',
                maxHeight: '300px',
              }}>
                <JsonTree data={event.log_attributes} defaultExpanded={true} />
              </div>
            </div>
          )}

          {/* Collapsible Raw JSON */}
          <div style={{ border: '1px solid #e2e8f0', borderRadius: '10px', overflow: 'hidden' }}>
            <button
              onClick={() => setJsonExpanded(!jsonExpanded)}
              style={{
                width: '100%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '14px 16px',
                background: '#f8fafc',
                border: 'none',
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: 600,
                color: '#475569',
              }}
            >
              <span>Raw Event JSON</span>
              <ChevronDown
                size={18}
                style={{
                  transform: jsonExpanded ? 'rotate(180deg)' : 'none',
                  transition: 'transform 0.2s ease',
                }}
              />
            </button>
            {jsonExpanded && (
              <div style={{
                background: '#1e293b',
                color: '#e2e8f0',
                padding: '16px',
                overflow: 'auto',
                maxHeight: '350px',
              }}>
                <JsonTree data={event} defaultExpanded={true} />
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// Searchable Dropdown Component (matching Query page style)
interface SearchableDropdownProps {
  label: string;
  value: string;
  options: string[];
  placeholder?: string;
  onChange: (value: string) => void;
}

function SearchableDropdown({ label, value, options, placeholder = 'All', onChange }: SearchableDropdownProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  // Close on click outside
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setIsOpen(false);
        setSearch('');
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const filtered = options.filter(o => o.toLowerCase().includes(search.toLowerCase()));

  return (
    <div>
      <label style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, marginBottom: '4px', display: 'block' }}>
        {label}
      </label>
      <div ref={ref} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: '4px' }}>
        <div
          onClick={() => setIsOpen(!isOpen)}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            padding: '6px 10px',
            background: 'white',
            border: '1px solid var(--border-color)',
            borderRadius: '6px',
            cursor: 'pointer',
            minWidth: '110px',
            transition: 'border-color 0.15s ease',
          }}
          onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
          onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}
        >
          <span style={{ flex: 1, fontSize: '12px', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: value ? 'var(--text)' : 'var(--text-muted)' }}>
            {value || placeholder}
          </span>
          <ChevronDown size={14} style={{ color: 'var(--text-muted)', flexShrink: 0, transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s ease' }} />
        </div>
        {value && (
          <button
            onClick={(e) => { e.stopPropagation(); onChange(''); }}
            style={{
              background: 'none',
              border: 'none',
              padding: '4px',
              cursor: 'pointer',
              color: 'var(--text-muted)',
              borderRadius: '4px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.background = '#fee2e2'; e.currentTarget.style.color = '#ef4444'; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; e.currentTarget.style.color = 'var(--text-muted)'; }}
            title="Clear filter"
          >
            <X size={14} />
          </button>
        )}

        {isOpen && (
          <div style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            right: 0,
            marginTop: '4px',
            background: 'white',
            border: '1px solid var(--border-color)',
            borderRadius: '6px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            zIndex: 100,
            maxHeight: '240px',
            display: 'flex',
            flexDirection: 'column',
          }}>
            {/* Search input */}
            <div style={{ padding: '8px', borderBottom: '1px solid var(--border-color)' }}>
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                padding: '6px 8px',
                background: 'var(--bg-secondary)',
                borderRadius: '4px',
              }}>
                <Search size={12} style={{ color: 'var(--text-muted)' }} />
                <input
                  type="text"
                  placeholder="Search..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  onClick={(e) => e.stopPropagation()}
                  autoFocus
                  style={{
                    flex: 1,
                    background: 'transparent',
                    border: 'none',
                    outline: 'none',
                    color: 'var(--text-primary)',
                    fontSize: '12px',
                  }}
                />
                {search && (
                  <button
                    onClick={(e) => { e.stopPropagation(); setSearch(''); }}
                    style={{ background: 'none', border: 'none', padding: '2px', cursor: 'pointer', color: 'var(--text-muted)' }}
                  >
                    <X size={10} />
                  </button>
                )}
              </div>
            </div>

            {/* Options list */}
            <div style={{ overflow: 'auto', flex: 1 }}>
              {/* All option */}
              <div
                onClick={() => { onChange(''); setIsOpen(false); setSearch(''); }}
                style={{
                  padding: '8px 12px',
                  fontSize: '12px',
                  cursor: 'pointer',
                  background: !value ? 'var(--accent-light)' : 'transparent',
                  fontWeight: !value ? 600 : 400,
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                onMouseLeave={(e) => e.currentTarget.style.background = !value ? 'var(--accent-light)' : 'transparent'}
              >
                {placeholder}
              </div>
              {filtered.map(opt => (
                <div
                  key={opt}
                  onClick={() => { onChange(opt); setIsOpen(false); setSearch(''); }}
                  style={{
                    padding: '8px 12px',
                    fontSize: '12px',
                    cursor: 'pointer',
                    background: value === opt ? 'var(--accent-light)' : 'transparent',
                    fontWeight: value === opt ? 600 : 400,
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                  onMouseLeave={(e) => e.currentTarget.style.background = value === opt ? 'var(--accent-light)' : 'transparent'}
                >
                  {opt}
                </div>
              ))}
              {filtered.length === 0 && (
                <div style={{ padding: '12px', fontSize: '12px', color: 'var(--text-muted)', textAlign: 'center' }}>
                  No matches
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// Time Range Picker with presets and custom range
interface TimeRange {
  startTime: number;
  endTime: number;
  label: string;
}

const PRESET_RANGES = [
  { label: 'All time', ms: 0 },
  { label: 'Last 5 min', ms: 5 * 60 * 1000 },
  { label: 'Last 15 min', ms: 15 * 60 * 1000 },
  { label: 'Last 30 min', ms: 30 * 60 * 1000 },
  { label: 'Last 1 hour', ms: 60 * 60 * 1000 },
  { label: 'Last 3 hours', ms: 3 * 60 * 60 * 1000 },
  { label: 'Last 12 hours', ms: 12 * 60 * 60 * 1000 },
  { label: 'Last 24 hours', ms: 24 * 60 * 60 * 1000 },
  { label: 'Last 7 days', ms: 7 * 24 * 60 * 60 * 1000 },
];

interface TimeRangePickerProps {
  value: TimeRange;
  onChange: (range: TimeRange) => void;
}

function TimeRangePicker({ value, onChange }: TimeRangePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [showCalendar, setShowCalendar] = useState(false);
  const [leftMonth, setLeftMonth] = useState(() => {
    const d = new Date();
    d.setMonth(d.getMonth() - 1);
    return d;
  });
  const [rightMonth, setRightMonth] = useState(() => new Date());
  const [selectedStart, setSelectedStart] = useState<Date | null>(null);
  const [selectedEnd, setSelectedEnd] = useState<Date | null>(null);
  const [selectingStart, setSelectingStart] = useState(true);
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setIsOpen(false);
        setShowCalendar(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const handlePreset = (preset: typeof PRESET_RANGES[0]) => {
    if (preset.ms === 0) {
      // "All time" - no time filter
      onChange({
        startTime: 0,
        endTime: 0,
        label: preset.label,
      });
    } else {
      const now = Date.now();
      onChange({
        startTime: now - preset.ms,
        endTime: now,
        label: preset.label,
      });
    }
    setIsOpen(false);
    setShowCalendar(false);
  };

  const handleDateSelect = (date: Date) => {
    if (selectingStart) {
      setSelectedStart(date);
      setSelectedEnd(null);
      setSelectingStart(false);
    } else {
      if (date < selectedStart!) {
        setSelectedStart(date);
        setSelectedEnd(selectedStart);
      } else {
        setSelectedEnd(date);
      }
      setSelectingStart(true);
    }
  };

  const handleCalendarApply = () => {
    if (selectedStart && selectedEnd) {
      const start = selectedStart.getTime();
      const end = selectedEnd.getTime() + (23 * 60 + 59) * 60 * 1000; // End of day
      const formatDate = (d: Date) => d.toLocaleDateString([], { month: 'short', day: 'numeric' });
      onChange({
        startTime: start,
        endTime: end,
        label: `${formatDate(selectedStart)} - ${formatDate(selectedEnd)}`,
      });
      setIsOpen(false);
      setShowCalendar(false);
      setSelectedStart(null);
      setSelectedEnd(null);
    }
  };

  const handleCustomApply = () => {
    if (customStart && customEnd) {
      let startDate = new Date(customStart);
      let endDate = new Date(customEnd);

      // Auto-swap if start > end
      if (startDate > endDate) {
        [startDate, endDate] = [endDate, startDate];
      }

      const formatDate = (d: Date) => d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      onChange({
        startTime: startDate.getTime(),
        endTime: endDate.getTime(),
        label: `${formatDate(startDate)} - ${formatDate(endDate)}`,
      });
      setIsOpen(false);
      setCustomStart('');
      setCustomEnd('');
    }
  };

  return (
    <div>
      <label style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, marginBottom: '6px', display: 'block', letterSpacing: '0.5px' }}>
        Time Range
      </label>
      <div ref={ref} style={{ position: 'relative' }}>
        {/* Trigger Button */}
        <div
          onClick={() => setIsOpen(!isOpen)}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            padding: '8px 12px',
            background: isOpen ? '#f8fafc' : 'white',
            border: isOpen ? '1px solid var(--accent-primary)' : '1px solid #e2e8f0',
            borderRadius: '8px',
            cursor: 'pointer',
            minWidth: '160px',
            transition: 'all 0.2s ease',
            boxShadow: isOpen ? '0 0 0 3px rgba(59, 130, 246, 0.1)' : '0 1px 2px rgba(0,0,0,0.05)',
          }}
          onMouseEnter={(e) => {
            if (!isOpen) {
              e.currentTarget.style.borderColor = '#94a3b8';
              e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.08)';
            }
          }}
          onMouseLeave={(e) => {
            if (!isOpen) {
              e.currentTarget.style.borderColor = '#e2e8f0';
              e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)';
            }
          }}
        >
          <Clock size={15} style={{ color: '#3b82f6', flexShrink: 0 }} />
          <span style={{ flex: 1, fontSize: '13px', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#1e293b' }}>
            {value.label}
          </span>
          <ChevronDown size={15} style={{ color: '#64748b', flexShrink: 0, transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s ease' }} />
        </div>

        {/* Dropdown Panel */}
        {isOpen && (
          <div style={{
            position: 'absolute',
            top: 'calc(100% + 6px)',
            right: 0,
            background: 'white',
            border: '1px solid #e2e8f0',
            borderRadius: '12px',
            boxShadow: '0 10px 40px rgba(0,0,0,0.12), 0 2px 6px rgba(0,0,0,0.08)',
            zIndex: 1000,
            width: '360px',
            padding: '16px',
            animation: 'fadeIn 0.15s ease',
          }}>
            {/* Quick Select Section */}
            <div style={{ marginBottom: '16px' }}>
              <div style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', fontWeight: 600, marginBottom: '10px', letterSpacing: '0.5px' }}>
                Quick Select
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '8px' }}>
                {PRESET_RANGES.map((preset) => {
                  const isSelected = value.label === preset.label;
                  return (
                    <button
                      key={preset.label}
                      onClick={() => handlePreset(preset)}
                      style={{
                        padding: '8px 6px',
                        fontSize: '11px',
                        background: isSelected ? 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)' : '#f8fafc',
                        color: isSelected ? 'white' : '#475569',
                        border: isSelected ? 'none' : '1px solid #e2e8f0',
                        borderRadius: '6px',
                        cursor: 'pointer',
                        transition: 'all 0.15s ease',
                        fontWeight: isSelected ? 600 : 500,
                        boxShadow: isSelected ? '0 2px 8px rgba(59, 130, 246, 0.35)' : 'none',
                      }}
                      onMouseEnter={(e) => {
                        if (!isSelected) {
                          e.currentTarget.style.background = '#f1f5f9';
                          e.currentTarget.style.borderColor = '#cbd5e1';
                        }
                      }}
                      onMouseLeave={(e) => {
                        if (!isSelected) {
                          e.currentTarget.style.background = '#f8fafc';
                          e.currentTarget.style.borderColor = '#e2e8f0';
                        }
                      }}
                    >
                      {preset.label}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Custom Range Section */}
            <div style={{ borderTop: '1px solid #e2e8f0', paddingTop: '16px' }}>
              <div style={{ fontSize: '11px', color: '#64748b', textTransform: 'uppercase', fontWeight: 600, marginBottom: '12px', letterSpacing: '0.5px' }}>
                Custom Range
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px', marginBottom: '14px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <label style={{ fontSize: '12px', color: '#64748b', width: '40px', fontWeight: 500 }}>From</label>
                  <input
                    type="datetime-local"
                    value={customStart}
                    onChange={(e) => setCustomStart(e.target.value)}
                    style={{
                      flex: 1,
                      padding: '8px 10px',
                      fontSize: '12px',
                      border: '1px solid #e2e8f0',
                      borderRadius: '6px',
                      background: '#f8fafc',
                      color: '#1e293b',
                      outline: 'none',
                      transition: 'all 0.15s ease',
                    }}
                    onFocus={(e) => {
                      e.currentTarget.style.borderColor = '#3b82f6';
                      e.currentTarget.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
                      e.currentTarget.style.background = 'white';
                    }}
                    onBlur={(e) => {
                      e.currentTarget.style.borderColor = '#e2e8f0';
                      e.currentTarget.style.boxShadow = 'none';
                      e.currentTarget.style.background = '#f8fafc';
                    }}
                  />
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                  <label style={{ fontSize: '12px', color: '#64748b', width: '40px', fontWeight: 500 }}>To</label>
                  <input
                    type="datetime-local"
                    value={customEnd}
                    onChange={(e) => setCustomEnd(e.target.value)}
                    style={{
                      flex: 1,
                      padding: '8px 10px',
                      fontSize: '12px',
                      border: '1px solid #e2e8f0',
                      borderRadius: '6px',
                      background: '#f8fafc',
                      color: '#1e293b',
                      outline: 'none',
                      transition: 'all 0.15s ease',
                    }}
                    onFocus={(e) => {
                      e.currentTarget.style.borderColor = '#3b82f6';
                      e.currentTarget.style.boxShadow = '0 0 0 3px rgba(59, 130, 246, 0.1)';
                      e.currentTarget.style.background = 'white';
                    }}
                    onBlur={(e) => {
                      e.currentTarget.style.borderColor = '#e2e8f0';
                      e.currentTarget.style.boxShadow = 'none';
                      e.currentTarget.style.background = '#f8fafc';
                    }}
                  />
                </div>
              </div>
              <div style={{ display: 'flex', gap: '10px', justifyContent: 'flex-end' }}>
                <button
                  type="button"
                  onClick={() => setIsOpen(false)}
                  style={{
                    padding: '8px 16px',
                    fontSize: '12px',
                    fontWeight: 500,
                    background: 'white',
                    color: '#64748b',
                    border: '1px solid #e2e8f0',
                    borderRadius: '6px',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.background = '#f8fafc';
                    e.currentTarget.style.borderColor = '#cbd5e1';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = 'white';
                    e.currentTarget.style.borderColor = '#e2e8f0';
                  }}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.preventDefault(); e.stopPropagation(); handleCustomApply(); }}
                  disabled={!customStart || !customEnd}
                  style={{
                    padding: '8px 16px',
                    fontSize: '12px',
                    fontWeight: 600,
                    background: customStart && customEnd ? 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)' : '#f1f5f9',
                    color: customStart && customEnd ? 'white' : '#94a3b8',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: customStart && customEnd ? 'pointer' : 'not-allowed',
                    boxShadow: customStart && customEnd ? '0 2px 8px rgba(59, 130, 246, 0.35)' : 'none',
                    transition: 'all 0.15s ease',
                  }}
                  onMouseEnter={(e) => {
                    if (customStart && customEnd) {
                      e.currentTarget.style.boxShadow = '0 4px 12px rgba(59, 130, 246, 0.4)';
                      e.currentTarget.style.transform = 'translateY(-1px)';
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (customStart && customEnd) {
                      e.currentTarget.style.boxShadow = '0 2px 8px rgba(59, 130, 246, 0.35)';
                      e.currentTarget.style.transform = 'translateY(0)';
                    }
                  }}
                >
                  Apply
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

interface DashboardStats {
  total_events: number;
  unique_users: number;
  events_today: number;
  events_last_hour: number;
}

interface UserEventCount {
  user_id: string;
  event_count: number;
  last_activity: string;
}

interface ResourceEventCount {
  resource_type: string;
  event_count: number;
  unique_users: number;
}

interface OperationCount {
  operation: string;
  status: string;
  event_count: number;
}

interface RecentEventsResponse {
  events: RecentEvent[];
  total_count: number;
}

// Dynamic filter options: attribute_name -> list of distinct values
type FilterOptions = Record<string, string[]>;

const PAGE_SIZE = 20;

// Default time range: Last 1 hour
const DEFAULT_TIME_RANGE: TimeRange = {
  startTime: Date.now() - 60 * 60 * 1000,
  endTime: Date.now(),
  label: 'Last 1 hour',
};

const API_BASE = 'http://localhost:8080/api/v1/dashboard';

function formatRelativeTime(dateStr: string): string {
  if (!dateStr) return '-';

  const date = new Date(dateStr);

  // Just show time for today, date+time otherwise
  const now = new Date();
  const isToday = date.toDateString() === now.toDateString();

  if (isToday) {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  });
}

export default function AccessLogs() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [userEvents, setUserEvents] = useState<UserEventCount[]>([]);
  const [resourceEvents, setResourceEvents] = useState<ResourceEventCount[]>([]);
  const [recentEvents, setRecentEvents] = useState<RecentEvent[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<RecentEvent | null>(null);
  const [totalEvents, setTotalEvents] = useState(0);
  const [operations, setOperations] = useState<OperationCount[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Dynamic filter options from backend: { attribute_name: [values] }
  const [filterOptions, setFilterOptions] = useState<FilterOptions>({});

  // Pagination and filtering state
  const [currentPage, setCurrentPage] = useState(0);
  const [timeRange, setTimeRange] = useState<TimeRange>(DEFAULT_TIME_RANGE);
  // Dynamic filters: { attribute_name: selected_value }
  const [filters, setFilters] = useState<Record<string, string>>({});

  const fetchFilterOptions = async (currentFilters: Record<string, string> = {}) => {
    try {
      // Pass current filters to get cascading options
      const params = new URLSearchParams();
      for (const [key, value] of Object.entries(currentFilters)) {
        if (value) params.set(`filter.${key}`, value);
      }
      const url = `${API_BASE}/filter-options${params.toString() ? '?' + params : ''}`;
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        setFilterOptions(data);
      }
    } catch (err) {
      console.error('Failed to load filter options:', err);
    }
  };

  const fetchData = async (
    page = currentPage,
    range = timeRange,
    activeFilters = filters
  ) => {
    setLoading(true);
    setError(null);

    const offset = page * PAGE_SIZE;
    const hasTimeFilter = range.startTime > 0 && range.endTime > 0;

    // Build filter params for recent events
    const params = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String(offset),
    });
    if (hasTimeFilter) {
      params.set('startTime', String(range.startTime));
      params.set('endTime', String(range.endTime));
    }
    // Add dynamic attribute filters with "filter." prefix
    for (const [attrName, attrValue] of Object.entries(activeFilters)) {
      if (attrValue) {
        params.set(`filter.${attrName}`, attrValue);
      }
    }

    // Time params for other endpoints (only if time filter is set)
    const timeParams = hasTimeFilter
      ? `startTime=${range.startTime}&endTime=${range.endTime}`
      : '';
    const timeQuery = timeParams ? `?${timeParams}` : '';
    const timeQueryAnd = timeParams ? `&${timeParams}` : '';

    try {
      const [statsRes, usersRes, resourcesRes, recentRes, opsRes] = await Promise.all([
        fetch(`${API_BASE}/stats${timeQuery}`),
        fetch(`${API_BASE}/events-by-user?limit=10${timeQueryAnd}`),
        fetch(`${API_BASE}/events-by-resource${timeQuery}`),
        fetch(`${API_BASE}/recent?${params}`),
        fetch(`${API_BASE}/operations${timeQuery}`),
      ]);

      if (!statsRes.ok) throw new Error('Failed to fetch stats');

      setStats(await statsRes.json());
      setUserEvents(await usersRes.json());
      setResourceEvents(await resourcesRes.json());

      const recentData: RecentEventsResponse = await recentRes.json();
      setRecentEvents(recentData.events || []);
      setTotalEvents(recentData.total_count || 0);

      setOperations(await opsRes.json());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleTimeRangeChange = (newRange: TimeRange) => {
    setTimeRange(newRange);
    setCurrentPage(0);
    fetchData(0, newRange, filters);
  };

  const handleFilterChange = (attrName: string, value: string) => {
    const newFilters = { ...filters };
    if (value) {
      newFilters[attrName] = value;
    } else {
      delete newFilters[attrName];
    }
    setFilters(newFilters);
    setCurrentPage(0);
    // Refresh filter options with new selections (cascading)
    fetchFilterOptions(newFilters);
    fetchData(0, timeRange, newFilters);
  };

  const handlePageChange = (newPage: number) => {
    setCurrentPage(newPage);
    fetchData(newPage, timeRange, filters);
  };

  const totalPages = Math.ceil(totalEvents / PAGE_SIZE);

  // Initial load and filter options fetch
  useEffect(() => {
    fetchFilterOptions();
    fetchData();
  }, []);

  // Auto-refresh with current filter state
  useEffect(() => {
    const interval = setInterval(() => {
      fetchData(currentPage, timeRange, filters);
    }, 30000);
    return () => clearInterval(interval);
  }, [currentPage, timeRange, filters]);

  if (loading && !stats) {
    return (
      <div className="loading">
        <div className="spinner" />
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-row">
          <div>
            <h1 className="page-title">Resource Access Log</h1>
            <p className="page-description">Real-time access event monitoring and analytics</p>
          </div>
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: '12px' }}>
            <TimeRangePicker
              value={timeRange}
              onChange={handleTimeRangeChange}
            />
            <button className="btn btn-secondary" onClick={() => fetchData()} disabled={loading}>
              <RefreshCw size={16} className={loading ? 'spin' : ''} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {error && (
        <div className="alert alert-error" style={{ marginBottom: '20px' }}>
          {error}
        </div>
      )}

      {/* Stats Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon stat-icon-blue">
            <Activity />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats?.total_events?.toLocaleString() ?? 0}</div>
            <div className="stat-label">Total Events</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-purple">
            <Users />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats?.unique_users ?? 0}</div>
            <div className="stat-label">Unique Users</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-amber">
            <TrendingUp />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats?.events_today ?? 0}</div>
            <div className="stat-label">Events Today</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-green">
            <Clock />
          </div>
          <div className="stat-content">
            <div className="stat-value">{stats?.events_last_hour ?? 0}</div>
            <div className="stat-label">Last Hour</div>
          </div>
        </div>
      </div>

      {/* Main Content Grid */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '24px', marginTop: '24px' }}>

        {/* Recent Events */}
        <div className="card">
          <div className="card-header" style={{ flexDirection: 'column', alignItems: 'stretch', gap: '12px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 className="card-title">Recent Events</h3>
              <Activity size={18} className="text-muted" />
            </div>
            {/* Active trace filter banner */}
            {filters.trace_id && (
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: '12px',
                padding: '10px 14px',
                background: 'linear-gradient(135deg, #f3e8ff 0%, #ede9fe 100%)',
                borderRadius: '8px',
                border: '1px solid #c4b5fd',
                marginBottom: '12px',
              }}>
                <Filter size={14} style={{ color: '#7c3aed' }} />
                <span style={{ fontSize: '13px', color: '#5b21b6', fontWeight: 500 }}>
                  Filtering by trace: <code style={{ background: '#e9d5ff', padding: '2px 6px', borderRadius: '4px', fontFamily: 'monospace', fontSize: '11px' }}>{filters.trace_id.substring(0, 16)}...</code>
                </span>
                <button
                  onClick={() => handleFilterChange('trace_id', '')}
                  style={{
                    marginLeft: 'auto',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px',
                    padding: '4px 10px',
                    background: '#7c3aed',
                    color: 'white',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: 'pointer',
                    fontSize: '12px',
                    fontWeight: 500,
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.background = '#6d28d9'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.background = '#7c3aed'; }}
                >
                  <X size={12} />
                  Clear
                </button>
              </div>
            )}
            <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', alignItems: 'flex-end' }}>
              {Object.entries(filterOptions).map(([attrName, values]) => (
                <SearchableDropdown
                  key={attrName}
                  label={attrName.replace(/_/g, ' ')}
                  value={filters[attrName] || ''}
                  options={values}
                  placeholder="All"
                  onChange={(v) => handleFilterChange(attrName, v)}
                />
              ))}
              {Object.keys(filterOptions).length === 0 && (
                <span className="text-muted text-sm">No filters available</span>
              )}
            </div>
          </div>
          <div className="card-body" style={{ padding: 0 }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>User</th>
                  <th>Resource</th>
                  <th>Operation</th>
                  <th>Status</th>
                  <th>Trace ID</th>
                  <th style={{ width: '40px' }}></th>
                </tr>
              </thead>
              <tbody>
                {recentEvents.length === 0 ? (
                  <tr>
                    <td colSpan={7} style={{ textAlign: 'center', padding: '20px' }}>
                      No events for {timeRange.label}
                    </td>
                  </tr>
                ) : (
                  recentEvents.map((event, idx) => (
                    <tr key={event.event_id || idx}>
                      <td title={new Date(event.timestamp).toLocaleString()}>
                        <span className="text-muted">{formatRelativeTime(event.timestamp)}</span>
                      </td>
                      <td>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                          <Users size={14} className="text-muted" />
                          <span>{event.user_id || '-'}</span>
                        </div>
                      </td>
                      <td>
                        <span className="tag">{event.resource_type || '-'}</span>
                      </td>
                      <td>{event.operation || '-'}</td>
                      <td>
                        <span className={`tag ${event.status === 'success' ? 'tag-success' : event.status === 'error' ? 'tag-error' : ''}`}>
                          {event.status || '-'}
                        </span>
                      </td>
                      <td>
                        {event.trace_id ? (
                          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <span
                              className="text-muted"
                              style={{ fontSize: '11px', fontFamily: 'monospace', cursor: 'pointer' }}
                              title="Click to copy"
                              onClick={() => navigator.clipboard.writeText(event.trace_id)}
                            >
                              {event.trace_id.substring(0, 8)}...
                            </span>
                            <a
                              href={`http://localhost:16686/trace/${event.trace_id}`}
                              target="_blank"
                              rel="noopener noreferrer"
                              style={{ fontSize: '10px', color: 'var(--accent-primary)', textDecoration: 'none' }}
                              title="View in Jaeger"
                            >
                              Traces ↗
                            </a>
                            <button
                              onClick={() => handleFilterChange('trace_id', event.trace_id)}
                              style={{
                                background: 'none',
                                border: 'none',
                                padding: '2px 4px',
                                cursor: 'pointer',
                                fontSize: '10px',
                                color: '#7c3aed',
                                borderRadius: '3px',
                              }}
                              onMouseEnter={(e) => { e.currentTarget.style.background = '#f3e8ff'; }}
                              onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; }}
                              title="Filter all logs by this trace"
                            >
                              <Filter size={12} />
                            </button>
                          </div>
                        ) : (
                          <span className="text-muted">-</span>
                        )}
                      </td>
                      <td>
                        <button
                          onClick={() => setSelectedEvent(event)}
                          style={{
                            background: 'none',
                            border: 'none',
                            padding: '4px',
                            cursor: 'pointer',
                            color: 'var(--text-muted)',
                            borderRadius: '4px',
                            display: 'flex',
                            alignItems: 'center',
                          }}
                          onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-secondary)'; e.currentTarget.style.color = 'var(--accent-primary)'; }}
                          onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; e.currentTarget.style.color = 'var(--text-muted)'; }}
                          title="View details"
                        >
                          <Eye size={16} />
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>

            {/* Pagination Controls */}
            {totalEvents > 0 && (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '12px 20px',
                  borderTop: '1px solid var(--border-light)',
                  background: 'var(--bg-secondary)',
                }}
              >
                <span className="text-sm text-muted">
                  Showing {currentPage * PAGE_SIZE + 1}-{Math.min((currentPage + 1) * PAGE_SIZE, totalEvents)} of {totalEvents}
                </span>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <button
                    className="btn btn-secondary"
                    onClick={() => handlePageChange(currentPage - 1)}
                    disabled={currentPage === 0 || loading}
                    style={{ padding: '6px 10px' }}
                  >
                    <ChevronLeft size={16} />
                  </button>
                  <span className="text-sm">
                    Page {currentPage + 1} of {totalPages}
                  </span>
                  <button
                    className="btn btn-secondary"
                    onClick={() => handlePageChange(currentPage + 1)}
                    disabled={currentPage >= totalPages - 1 || loading}
                    style={{ padding: '6px 10px' }}
                  >
                    <ChevronRight size={16} />
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Right Column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>

          {/* Events by User */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">Top Users</h3>
              <Users size={18} className="text-muted" />
            </div>
            <div className="card-body" style={{ padding: 0 }}>
              {userEvents.length === 0 ? (
                <div style={{ padding: '20px', textAlign: 'center' }}>No data</div>
              ) : (
                userEvents.map((user, idx) => (
                  <div
                    key={user.user_id || idx}
                    style={{
                      padding: '12px 20px',
                      borderBottom: '1px solid var(--border-light)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 500 }}>{user.user_id}</div>
                      <div className="text-sm text-muted">
                        Last: {formatRelativeTime(user.last_activity)}
                      </div>
                    </div>
                    <div className="tag">{user.event_count} events</div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Events by Resource Type */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">By Resource Type</h3>
              <Database size={18} className="text-muted" />
            </div>
            <div className="card-body" style={{ padding: 0 }}>
              {resourceEvents.length === 0 ? (
                <div style={{ padding: '20px', textAlign: 'center' }}>No data</div>
              ) : (
                resourceEvents.map((resource, idx) => (
                  <div
                    key={resource.resource_type || idx}
                    style={{
                      padding: '12px 20px',
                      borderBottom: '1px solid var(--border-light)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 500 }}>{resource.resource_type}</div>
                      <div className="text-sm text-muted">
                        {resource.unique_users} users
                      </div>
                    </div>
                    <div className="tag">{resource.event_count}</div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Operations Breakdown */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">Operations</h3>
              <Filter size={18} className="text-muted" />
            </div>
            <div className="card-body" style={{ padding: 0 }}>
              {operations.length === 0 ? (
                <div style={{ padding: '20px', textAlign: 'center' }}>No data</div>
              ) : (
                operations.slice(0, 8).map((op, idx) => (
                  <div
                    key={`${op.operation}-${op.status}-${idx}`}
                    style={{
                      padding: '10px 20px',
                      borderBottom: '1px solid var(--border-light)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>{op.operation}</span>
                      <span className={`tag tag-sm ${op.status === 'success' ? 'tag-success' : ''}`}>
                        {op.status}
                      </span>
                    </div>
                    <span className="text-muted">{op.event_count}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Log Detail Modal */}
      {selectedEvent && (
        <LogDetailModal
          event={selectedEvent}
          onClose={() => setSelectedEvent(null)}
          onFilterByTrace={(traceId) => handleFilterChange('trace_id', traceId)}
        />
      )}
    </div>
  );
}
