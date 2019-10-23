declare module 'gnip' {
	interface GnipStreamOptions {
		url: string
		user: string
		password: string
		backfillMinutes?: number
	}

	export interface GnipRulesOptions {
		url: string
		user: string
		password: string
		batchSize?: number
	}

	export interface PowertrackRule {
		value?: string
		tag?: string
		id?: number | string
		id_str?: string
	}

	export interface PowertrackResultsDetail {
		rule: PowertrackRule
		created: boolean
		message: string
	}

	export interface PowertrackAddRemoveRuleResult {
		summary: {
			created?: number
			not_created?: number
			deleted?: number
			not_deleted?: number
		}
		detail: PowertrackResultsDetail[]
		sent: Date
	}

	export interface LiveRules {
		getAll(): Promise<PowertrackRule[]>
		getByIds(): Promise<PowertrackRule[]>
		add(rules: string[] | PowertrackRule[]): Promise<PowertrackAddRemoveRuleResult>
		removeByIds(rules: string[] | PowertrackRule[]): Promise<PowertrackAddRemoveRuleResult>
	}

	export class Rules {
		public constructor(gnipRulesOptions: GnipRulesOptions)

		public live: LiveRules
	}

	export class Stream {
		public constructor(gnipStreamOptions: GnipStreamOptions)

		public on(event: 'ready', callback: (...args: any[]) => any): void
		public on<T>(event: 'tweet', callback: (tweet: T) => any): void
		public on(event: 'info', callback: (info: any) => any): void
		public on(event: 'error', callback: (...args: any[]) => any): void
		public on(event: 'end', callback: () => any): void

		public off(event: 'ready' | 'tweet' | 'info' | 'error' | 'end', callback: Function): void

		public start(): void
		public end(): void
	}
}
