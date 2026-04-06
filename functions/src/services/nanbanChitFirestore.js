/**
 * Chit fund data lives in businesses/Nanban/snapshot/main → chitData.
 */

function defaultChit() {
  return { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
}

function normalizeChit(raw) {
  const chit = { ...defaultChit(), ...(raw && typeof raw === "object" ? raw : {}) };
  for (const k of Object.keys(defaultChit())) {
    if (!Array.isArray(chit[k])) chit[k] = [];
  }
  return chit;
}

function buildMemberChitPassbook(memberName, chit) {
  const name = String(memberName || "").trim();
  if (!name) return { status: "error", message: "Name required" };

  const members = chit.members || [];
  const auctions = chit.auctions || [];
  const payments = chit.payments || [];

  const memberEntries = members.filter((m) => m && m.name === name);
  const groupSummaries = memberEntries.map((m) => {
    const groupAuctions = auctions
      .filter((a) => a && a.group === m.group)
      .sort((a, b) => (parseInt(a.month, 10) || 0) - (parseInt(b.month, 10) || 0));
    const winEntry = groupAuctions.find((a) => a.winner === m.name);

    const payRows = groupAuctions.map((a) => {
      const pay = payments.find((p) => String(p.auctionId) === String(a.id) && p.memberName === m.name);
      return {
        month: a.month,
        date: a.date,
        perHead: a.perHead,
        paid: !!pay,
        paidAmt: pay ? pay.amount : 0,
        receiver: pay ? pay.receiver : ""
      };
    });

    const totalPaid = payRows.reduce((s, p) => s + (parseInt(p.paidAmt, 10) || 0), 0);
    const pendingMonths = payRows.filter((p) => !p.paid).length;

    return {
      group: m.group,
      joinedBy: m.joinedBy,
      joinDate: m.date,
      won: !!winEntry,
      wonMonth: winEntry ? winEntry.month : null,
      payments: payRows,
      totalPaid,
      pendingMonths
    };
  });

  return { status: "success", name, groups: groupSummaries };
}

/**
 * Fix payments where auctionId was mistakenly stored as group name (legacy sheet quirk).
 */
function fixHistoricalChitPayments(chit) {
  const auctions = Array.isArray(chit.auctions) ? chit.auctions : [];
  const payments = Array.isArray(chit.payments) ? chit.payments : [];
  let fixes = 0;

  const next = payments.map((p) => {
    if (!p) return p;
    const aid = String(p.auctionId || "");
    if (!aid || !/[a-zA-Z\s]/.test(aid)) return p;
    const groupName = aid;
    const monthNo = p.month != null ? String(p.month) : "";
    const correct = auctions.find((a) => String(a.group) === groupName && String(a.month) === monthNo);
    if (correct && String(correct.id)) {
      fixes++;
      return { ...p, auctionId: String(correct.id) };
    }
    return p;
  });

  return { chit: { ...chit, payments: next }, fixes };
}

module.exports = {
  defaultChit,
  normalizeChit,
  buildMemberChitPassbook,
  fixHistoricalChitPayments
};
