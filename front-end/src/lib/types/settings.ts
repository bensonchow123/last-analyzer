// Mirror of what each service returns from GET /settings. The fields array is
// the service's own settings_spec.json, so a new setting is a backend json edit
// and this page renders it without changing.

export type SettingType = 'string' | 'int' | 'bool';

export type SettingField = {
	key: string;
	label: string;
	type?: SettingType;
	secret?: boolean;
	required?: boolean; // nothing works until this one is filled in
	help?: string;
	min?: number;
	max?: number;
};

// Secrets come back as a hint only, the value itself never leaves the service
export type SecretValue = { set: boolean; preview: string };

export type SettingValue = string | number | boolean | null | SecretValue;

// 'env' means the value still comes from .env, 'settings' means this page owns it
export type SettingSource = 'env' | 'settings';

export type ServiceSettings = {
	service: string;
	fields: SettingField[];
	values: Record<string, SettingValue>;
	sources: Record<string, SettingSource>;
	missing: string[]; // required keys still unset, so a fresh install can be led through setup
	auth_required: boolean; // false when no ADMIN_API_TOKEN is set on that service
};

export type ServiceId = 'vault' | 'llm';

// One card on the settings page, either loaded or saying why it is not
export type ServicePanel = {
	id: ServiceId;
	name: string;
	settings: ServiceSettings | null;
	error: string | null;
};

export function isSecretValue(value: SettingValue): value is SecretValue {
	return typeof value === 'object' && value !== null && 'set' in value;
}
