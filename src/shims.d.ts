// `saslmechanisms` (a transitive dep of @xmpp/sasl) ships no type declarations.
// We only touch its prototype to reorder mechanism preference; a loose shape is enough.
declare module "saslmechanisms" {
	interface SASLFactory {
		use(...args: unknown[]): SASLFactory;
	}
	const SASLFactory: { new (): SASLFactory; prototype: SASLFactory };
	export default SASLFactory;
}
